package doublebuffer

import (
	"context"
	"errors"
	"io"
	"sync"
)

var bytesPool sync.Pool = sync.Pool{
	New: func() any {
		return []byte(nil)
	},
}

type DoubleBuffer struct {
	dataReadyFlag   chan struct{}
	bufferReadyFlag chan struct{}
	backBuffer      []byte
	bufferLimit     int
	bufferMutex     sync.Mutex
	context         context.Context
	cancel          context.CancelCauseFunc
}

func NewWriterSize(wr io.Writer, size int) *DoubleBuffer {
	db := &DoubleBuffer{
		dataReadyFlag:   make(chan struct{}, 1),
		bufferReadyFlag: make(chan struct{}, 1),
		backBuffer:      bytesPool.Get().([]byte)[:0],
		bufferLimit:     size,
	}
	db.context, db.cancel = context.WithCancelCause(context.Background())
	go db.flusher(wr)
	return db
}

func (db *DoubleBuffer) flusher(wr io.Writer) {
	defer func() {
		var err error
		wc, ok := wr.(io.WriteCloser)
		if ok {
			err = wc.Close()
			if err == nil {
				err = io.ErrClosedPipe
			}
		}
		db.cancel(err)
	}()

	frontBuffer := bytesPool.Get().([]byte)
	defer bytesPool.Put(frontBuffer)

	for range db.dataReadyFlag {
		db.bufferMutex.Lock()
		frontBuffer, db.backBuffer = db.backBuffer, frontBuffer[:0]
		db.bufferMutex.Unlock()
		select {
		case db.bufferReadyFlag <- struct{}{}:
		default:
		}
		if len(frontBuffer) == 0 {
			continue
		}
		_, err := wr.Write(frontBuffer)
		if err != nil {
			wc, ok := wr.(io.WriteCloser)
			if ok {
				wc.Close()
			}
			db.cancel(err)
			return
		}
	}
}

func (db *DoubleBuffer) Close() error {
	select {
	case <-db.context.Done():
		return db.context.Err()
	default:
	}
	close(db.dataReadyFlag)
	<-db.context.Done()
	err := db.context.Err()
	if errors.Is(err, io.ErrClosedPipe) {
		err = nil
	}
	return err
}

func (db *DoubleBuffer) Write(p []byte) (n int, err error) {
	if len(p) > db.bufferLimit {
		totalWritten := 0
		for len(p) > 0 {
			written, err := db.Write(p[:min(db.bufferLimit, len(p))])
			totalWritten += written
			if err != nil {
				return totalWritten, err
			}
			p = p[written:]
		}
		return totalWritten, nil
	}
	for {
		select {
		case <-db.context.Done():
			return 0, db.context.Err()
		default:
		}
		db.bufferMutex.Lock()
		if len(db.backBuffer)+len(p) > db.bufferLimit {
			db.bufferMutex.Unlock()
			select {
			case <-db.context.Done():
				return 0, db.context.Err()
			case <-db.bufferReadyFlag:
			}
			continue
		}
		db.backBuffer = append(db.backBuffer, p...)
		db.bufferMutex.Unlock()
		select {
		case db.dataReadyFlag <- struct{}{}:
		default:
		}
		return len(p), nil
	}
}

func (db *DoubleBuffer) Reset(wr io.Writer) {
	db.Close()
	db.backBuffer = db.backBuffer[:0]
	db.dataReadyFlag = make(chan struct{}, 1)
	db.context, db.cancel = context.WithCancelCause(context.Background())
	go db.flusher(wr)
}
