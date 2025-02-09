package doublebuffer

import (
	"context"
	"io"
	"sync"
)

var bytesPool sync.Pool = sync.Pool{
	New: func() any {
		return []byte(nil)
	},
}

type DoubleBuffer struct {
	w               io.WriteCloser
	dataReadyFlag   chan struct{}
	bufferReadyFlag chan struct{}
	backBuffer      []byte
	bufferLimit     int
	bufferMutex     sync.Mutex
	cancelMutex     sync.Mutex
	context         context.Context
	cancel          context.CancelCauseFunc
}

func NewWriterSize(w io.WriteCloser, size int) *DoubleBuffer {
	db := &DoubleBuffer{
		w:               w,
		dataReadyFlag:   make(chan struct{}, 1),
		bufferReadyFlag: make(chan struct{}, 1),
		backBuffer:      bytesPool.Get().([]byte)[:0],
		bufferLimit:     size,
	}
	db.context, db.cancel = context.WithCancelCause(context.Background())
	go db.flusher()
	return db
}

func (db *DoubleBuffer) flusher() {
	defer func() {
		db.cancel(db.w.Close())
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
		_, err := db.w.Write(frontBuffer)
		if err != nil {
			db.w.Close()
			db.cancel(err)
			return
		}
	}
}

func (db *DoubleBuffer) Close() error {
	db.cancelMutex.Lock()
	defer db.cancelMutex.Unlock()
	select {
	case <-db.context.Done():
	default:
		close(db.dataReadyFlag)
		<-db.context.Done()
	}
	return db.context.Err()
}

func (db *DoubleBuffer) Write(p []byte) (int, error) {
	if len(p) > db.bufferLimit {
		totalWritten := 0
		for len(p) > 0 {
			written, err := db.Write(p[:db.bufferLimit])
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
