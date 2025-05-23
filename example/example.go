package main

import (
	"fmt"
	"github.com/Fusl/go-resp"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"unsafe"
)

func BytesToLower(b []byte) []byte {
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return b
}

// bstring converts a byte slice to a string without copying.
func bstring(bs []byte) string {
	p := unsafe.SliceData(bs)
	return unsafe.String(p, len(bs))
}

func main() {
	go func() {
		// pprof server for profiling
		http.ListenAndServe("127.0.0.1:6060", nil)
	}()
	l, err := net.Listen("tcp", ":6380")
	if err != nil {
		panic(err)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			panic(err)
		}
		go func() {
			defer conn.Close()
			// Wrap the TCP connection in a RESP client connection
			rconn := resp.NewServer(conn)
			defer rconn.Close()

			log.Printf("opened connection from %s", conn.RemoteAddr())

			if err := rconn.SetOptions(resp.ServerOptions{
				MaxMultiBulkLength: resp.Pointer(1024),
				MaxBulkLength:      resp.Pointer(65536),
				MaxBufferSize:      resp.Pointer(1048576),
			}); err != nil {
				rconn.CloseWithError(err)
			}
			for {
				// Read the next command line arguments
				args, err := rconn.Next()
				if err != nil {
					rconn.CloseWithError(err)
					log.Printf("closed connection from %s during read: %v", conn.RemoteAddr(), err)
					return
				}

				// Pull the command from the arguments and convert it to lowercase
				cmd := bstring(BytesToLower(args[0]))
				args = args[1:]

				// Handle the command
				switch cmd {
				case "ping":
					// Write a status string response
					rconn.WriteStatusString("PONG")
				case "echo":
					if len(args) == 0 {
						rconn.WriteError(fmt.Errorf("wrong number of arguments for 'echo' command"))
						continue
					}
					if len(args) == 1 {
						// Write a bulk string response
						rconn.WriteBytes(args[0])
						continue
					}
					// Write a multibulk string response
					rconn.WriteArrayBytes(args)
				case "test":
					// manually write an array of strings
					rconn.WriteArrayHeader(2)
					rconn.WriteStatusString("hello")
					rconn.WriteStatusString("world")
				case "quit":
					// Write a status string response
					rconn.WriteStatusString("OK")
					return
				default:
					// Write an error response
					rconn.WriteError(fmt.Errorf("unknown command '%s'", cmd))
				}

				if err := rconn.WriteRaw(nil); err != nil {
					log.Printf("closed connection from %s during write: %v", conn.RemoteAddr(), err)
					return
				}
			}
		}()
	}
}
