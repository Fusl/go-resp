# go-resp

go-resp is a [fast](#Benchmarks) and simple, barebones [RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/) (Redis serialization protocol) parser and serializer for Go.

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

Depending on the state of the internally allocated read buffer, writing may not immediately flush to the client, such as to improve performance when processing pipelined commands. To immediately flush any pending writes, call `Flush()` on the `ClientConn` instance.

To prevent flushing after every write, such as when manually writing an array response with a large number of entries, call the `Buffer()` method on the `ClientConn` instance which accepts a function that writes the response:

```go
rconn.WriteBuffered(func() error {
    rconn.WriteArrayHeader(2)
    rconn.WriteStatusString("hello")
    rconn.WriteStatusString("world")
    return nil
})
```

### Concurrency

Reading or writing is not thread-safe and should not be done concurrently. You may read with `Next()` in one goroutine and write with `Write()` in another goroutine, but you should not call `Next()` or `Write()` concurrently.

## Example

See [example/example.go](example/example.go) for a simple example server.

## Benchmarks

### Benchmark information

#### Hardware

- CPU: Intel Core i9-13900KF @ 5.5GHz
- RAM: 4x 32GB DDR4 @ 3200MHz

#### Benchmark command
```sh
$ redis-benchmark -p <port> -c <clients> -P <numreq> -n 100000000 ping
```

### Standard Redis Server

```sh
$ redis-server --port 6379 --appendonly no --save ''
```

| -c |   -P | Throughput | latency | min   | p50   | p95   | p99   | max   |
|---:|-----:|-----------:|---------|-------|-------|-------|-------|-------|
|  1 |    1 |    161,421 | 0.004   | 0.000 | 0.007 | 0.007 | 0.007 | 0.303 |
|  1 | 1000 |  6,930,488 | 0.112   | 0.104 | 0.119 | 0.119 | 0.119 | 0.279 |
| 10 | 1000 |  9,683,355 | 0.935   | 0.112 | 0.935 | 1.007 | 1.015 | 1.679 |

### go-resp example server (GOMAXPROCS=8)

```sh
$ GOMAXPROCS=8 go run ./example/example.go
```

| -c |   -P | Throughput | Increase | latency | min   | p50   | p95   | p99   | max   |
|---:|-----:|-----------:|---------:|---------|-------|-------|-------|-------|-------|
|  1 |    1 |    167,788 |   +3.94% | 0.004   | 0.000 | 0.007 | 0.007 | 0.007 | 0.439 |
|  1 | 1000 | 10,921,800 |  +57.59% | 0.059   | 0.048 | 0.063 | 0.071 | 0.071 | 0.215 |
| 10 | 1000 | 30,693,678 | +216.97% | 0.200   | 0.048 | 0.199 | 0.311 | 0.359 | 0.815 |

### go-resp example server (GOMAXPROCS=1)

```sh
$ GOMAXPROCS=1 go run ./example/example.go
```

| -c |   -P | Throughput | Increase | latency | min   | p50   | p95   | p99   |   max |
|---:|-----:|-----------:|---------:|---------|-------|-------|-------|-------|------:|
|  1 |    1 |    167,546 |   +3.79% | 0.004   | 0.000 | 0.007 | 0.007 | 0.007 | 0.415 |
|  1 | 1000 | 10,629,252 |  +53.36% | 0.061   | 0.056 | 0.063 | 0.071 | 0.071 | 0.167 |
| 10 | 1000 | 18,811,136 |  +94.26% | 0.499   | 0.056 | 0.503 | 0.927 | 0.935 | 1.191 |
