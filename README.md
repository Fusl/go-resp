# go-resp

go-resp is a fast and simple, barebones [RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/) (Redis serialization protocol) parser and serializer for Go.

## Installing

```
go get -u github.com/Fusl/go-resp
```

## Usage

`resp.NewClientConn()` accepts a `net.Conn` and returns a `resp.ClientConn` which can be used to read and write RESP messages.

### Reading

Every `ClientConn` instance has a `Next()` method which reads the next RESP message from the connection. The method returns a `[][]byte` slice containing the RESP message and an error. Care must be taken when reading the message as the slice and the underlying byte slices are reused for every call to `Next()`.  If in doubt, copy the slice and every byte slice it contains to a newly allocated slice and byte slices.

### Writing

Every `ClientConn` instance has various write methods for writing RESP messages to the connection. The methods are named after the RESP message type they write. By default, messages are written in RESP3 compatible format. Calling `SetRESP2Compat(true)` on a `ClientConn` instance will make it write messages in RESP2 compatible format instead, even when calling RESP3 methods such as `WriteMapBytes()` which are not available in RESP2 and therefore converted to arrays.

See the [go-resp package documentation](https://pkg.go.dev/github.com/Fusl/go-resp) for a list of all available write methods and how to use them.

## Example

See [example/example.go](example/example.go) for a simple example server.

## Benchmarks

### Standard Redis Server

```sh
$ redis-server --port 6379 --appendonly no --save ''
```

```sh
$ redis-benchmark -p 6379 -c 1 -P 1 -n 1000000 ping
  throughput summary: 55635.91 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.013     0.008     0.015     0.023     0.031     0.807
        
$ redis-benchmark -p 6379 -c 1 -d 1 -P 1000 -n 100000000 ping
  throughput summary: 4169620.25 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.196     0.176     0.191     0.215     0.295     3.743

$ redis-benchmark -p 6379 -c 10 -d 1 -P 1000 -n 100000000 ping
  throughput summary: 5554629.50 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        1.651     0.248     1.623     1.775     2.863     4.223
```

### go-resp example server

```sh
$ go run ./example/example.go
```

```sh
$ redis-benchmark -p 6380 -c 1 -P 1 -n 1000000 ping
Summary:
  throughput summary: 56325.34 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.014     0.008     0.015     0.023     0.031     0.535

$ redis-benchmark -p 6380 -c 1 -d 1 -P 1000 -n 100000000 ping
  throughput summary: 7256894.00 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.075     0.056     0.071     0.111     0.167     0.735

$ redis-benchmark -p 6380 -c 10 -d 1 -P 1000 -n 100000000 ping
  throughput summary: 17551558.00 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        1.462     0.632     1.423     1.895     2.159     5.687

```