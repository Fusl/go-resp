package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/Fusl/go-resp"
	"net"
)

func main() {
	l, err := net.Listen("tcp", ":6379")
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
			rconn := resp.NewClientConn(conn)
			defer rconn.Close()
			for {
				// Read the next command line arguments
				args, err := rconn.Next()
				if err != nil {
					fmt.Println(err)
					return
				}

				// Pull the command from the arguments and convert it to lowercase
				cmd := string(bytes.ToLower(args[0]))
				args := args[1:]

				// Handle the command
				switch cmd {
				case "ping":
					// Write a status string response
					rconn.WriteStatusString("PONG")
				case "echo":
					if len(args) != 1 {
						rconn.WriteBlobError(errors.New("wrong number of arguments for 'echo' command"))
						continue
					}
					// Write a bulk string response
					rconn.WriteBytes(args[0])
				default:
					// Write an error response
					rconn.WriteError(fmt.Errorf("unknown command '%s'", cmd))
				}
			}
		}()
	}
}
