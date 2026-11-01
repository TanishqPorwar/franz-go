Schema registry protobuf serde comparison
===

This compares franz-go's `plugin/srproto` against confluent-kafka-go
v2's `schemaregistry/serde/protobuf`, encoding and decoding the same message
against the same in-memory registry (`pkg/sr/srfake`). No broker is involved:
this measures the serde path only.

```
CGO_ENABLED=1 go test -bench . -benchmem
```

`CGO_ENABLED=1` is required by confluent-kafka-go alone: its `schemaregistry`
package reads a version string out of its cgo `kafka` package, so it does not
build without cgo even though nothing here talks to a broker. franz-go itself
is pure Go and needs nothing from cgo. Because both serdes are benchmarked in
one test binary, that setting applies to the whole package.

Running the two halves under different `CGO_ENABLED` settings would be worse,
not better: they would no longer share a runtime, a GC configuration, or a
single run's thermal conditions. `CGO_ENABLED` does not change code generation
for pure Go functions anyway. Toggling it over the franz-go-only benchmarks in
`plugin/srproto` gives bit-identical allocation counts and no
measurable time difference; what shows up there is ordering noise, where
whichever configuration runs second looks ~4% faster. For a cgo-free
measurement of this repo's serde on its own, use that package's benchmarks
rather than splitting this one.

## Results

Apple M3 Pro, go1.27, confluent-kafka-go v2.15.1:

```
BenchmarkFranzGoEncode-12           19587852      120.9 ns/op     32 B/op    2 allocs/op
BenchmarkFranzGoAppendEncode-12     24621202       95.00 ns/op     0 B/op    0 allocs/op
BenchmarkConfluentEncode-12           356828     6763    ns/op   5993 B/op  145 allocs/op

BenchmarkFranzGoDecode-12           18666964      134.1 ns/op     88 B/op    5 allocs/op
BenchmarkConfluentDecodeInto-12      5019649      461.7 ns/op    272 B/op    9 allocs/op

BenchmarkFranzGoDecodeNew-12        12045410      202.9 ns/op    184 B/op    6 allocs/op
BenchmarkConfluentDeserialize-12     4760803      503.5 ns/op    368 B/op   10 allocs/op
```

Both clients cache the schema id after the first record, so none of these
numbers include a registry round trip; `TestRegistryCallsAreCached` asserts
that, so the encode gap is local CPU work rather than one client doing I/O
the other avoids.

`AppendEncode` has no confluent-kafka-go equivalent. It appends into a
caller-owned buffer, which is what gets the steady state path to zero
allocations.

## Interop

`TestSerdesInterop` points both clients at one registry and decodes each
client's output with the other, so the framing and message indexes are
checked rather than assumed.

`TestCodegenOptionStripping` covers the one deliberate difference. franz-go
strips `go_package` and the other codegen options before registering, since
they describe the producer's code generation rather than the message, and a
Java producer of the same message would not send them. confluent-kafka-go
registers the schema text verbatim. On a shared subject those are two
different schema texts and therefore two different ids. Pass
`srproto.KeepCodegenOptions()` to register verbatim and land on the exact
same id as an existing confluent-kafka-go producer; the test asserts the
wire bytes are then byte-identical.
