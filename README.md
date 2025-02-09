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
|  1 |    1 |    169,964 |   +5.29% | 0.004   | 0.000 | 0.007 | 0.007 | 0.007 | 0.311 |
|  1 | 1000 | 11,774,402 |  +69.89% | 0.052   | 0.048 | 0.055 | 0.055 | 0.055 | 0.159 |
| 10 | 1000 | 31,318,508 | +223.43% | 0.188   | 0.048 | 0.183 | 0.271 | 0.319 | 0.567 |

### go-resp example server (GOMAXPROCS=1)

```sh
$ GOMAXPROCS=1 go run ./example/example.go
```

| -c |   -P | Throughput | Increase | latency | min   | p50   | p95   | p99   |   max |
|---:|-----:|-----------:|---------:|---------|-------|-------|-------|-------|------:|
|  1 |    1 |    169,330 |   +4.90% | 0.004   | 0.000 | 0.007 | 0.007 | 0.007 | 0.271 |
|  1 | 1000 | 11,970,313 |  +72.72% | 0.052   | 0.040 | 0.055 | 0.055 | 0.055 | 0.199 |
| 10 | 1000 | 22,456,770 | +131.91% | 0.412   | 0.040 | 0.415 | 0.775 | 0.791 | 0.903 |
