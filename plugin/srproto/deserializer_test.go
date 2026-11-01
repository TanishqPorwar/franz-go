package srproto

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"
	"github.com/twmb/franz-go/plugin/srproto/testpb"
)

// countingRegistry is a srfake registry that tallies requests, so tests can
// assert on how much registry traffic a decode actually causes.
type countingRegistry struct {
	*srfake.Registry
	mu     sync.Mutex
	counts map[string]int
}

func newRegistry(t *testing.T) *countingRegistry {
	t.Helper()
	c := &countingRegistry{counts: make(map[string]int)}
	c.Registry = srfake.New()
	t.Cleanup(c.Close)
	c.Intercept(func(_ http.ResponseWriter, r *http.Request) bool {
		c.mu.Lock()
		c.counts[r.Method+" "+r.URL.Path]++
		c.mu.Unlock()
		return false
	})
	return c
}

func (c *countingRegistry) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var n int
	for _, v := range c.counts {
		n += v
	}
	return n
}

func (c *countingRegistry) reset() {
	c.mu.Lock()
	c.counts = make(map[string]int)
	c.mu.Unlock()
}

func (c *countingRegistry) client(t *testing.T) *sr.Client {
	t.Helper()
	cl, err := sr.NewClient(sr.URLs(c.URL()))
	if err != nil {
		t.Fatalf("sr.NewClient: %v", err)
	}
	return cl
}

func protoSchema(text string, refs ...sr.SchemaReference) sr.Schema {
	return sr.Schema{Schema: text, Type: sr.TypeProtobuf, References: refs}
}

// frame builds a record the way a producer would: the Confluent header
// followed by the marshaled message. PR 2 has no serializer, so tests that
// need a framed record build one here.
func frame(t *testing.T, id int, index []int, msg proto.Message) []byte {
	t.Helper()
	var header sr.ConfluentHeader
	b, err := header.AppendEncode(nil, id, index)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return append(b, payload...)
}

func newDeser(t *testing.T, cl *sr.Client, opts ...Opt) *Deserializer {
	t.Helper()
	d, err := NewDeserializer(cl, opts...)
	if err != nil {
		t.Fatalf("NewDeserializer: %v", err)
	}
	return d
}

const widgetProto = `syntax = "proto3";
package testpb;
message Widget { string name = 1; }
message Pizza { int32 slices = 1; }`

func TestDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	reg := newRegistry(t)
	reg.SeedSchema("widgets-value", 1, 1, protoSchema(widgetProto))
	d := newDeser(t, reg.client(t))

	b := frame(t, 1, []int{0}, &testpb.Widget{Name: "gear"})

	var got testpb.Widget
	if err := d.Decode(context.Background(), b, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.GetName() != "gear" {
		t.Errorf("name = %q, want %q", got.GetName(), "gear")
	}
}

// Decode into a caller-supplied value is the hot consumer path: it knows the
// type already, so it should not talk to the registry at all.
func TestDecodeMakesNoRegistryRequests(t *testing.T) {
	t.Parallel()

	reg := newRegistry(t)
	reg.SeedSchema("widgets-value", 1, 1, protoSchema(widgetProto))
	d := newDeser(t, reg.client(t))
	b := frame(t, 1, []int{0}, &testpb.Widget{Name: "gear"})
	reg.reset()

	for range 50 {
		var got testpb.Widget
		if err := d.Decode(context.Background(), b, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
	}
	if n := reg.total(); n != 0 {
		t.Errorf("%d registry requests across 50 decodes, want 0: %v", n, reg.counts)
	}
}

func TestVerifyDecodeRejectsAMismatchedType(t *testing.T) {
	t.Parallel()

	reg := newRegistry(t)
	reg.SeedSchema("widgets-value", 1, 1, protoSchema(widgetProto))
	d := newDeser(t, reg.client(t), VerifyDecode())

	// Index 1 is Pizza, but we decode into Widget.
	b := frame(t, 1, []int{1}, &testpb.Pizza{Slices: 8})

	var got testpb.Widget
	err := d.Decode(context.Background(), b, &got)
	if err == nil {
		t.Fatal("Decode accepted a record whose schema index names a different message")
	}
	for _, want := range []string{"testpb.Pizza", "testpb.Widget"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}

	// Without VerifyDecode the same record decodes silently, which is the
	// trade the option exists to let callers make.
	plain := newDeser(t, reg.client(t))
	var quiet testpb.Widget
	if err := plain.Decode(context.Background(), b, &quiet); err != nil {
		t.Fatalf("Decode without VerifyDecode: %v", err)
	}
}

func TestVerifyDecodeAcceptsAMatchingType(t *testing.T) {
	t.Parallel()

	reg := newRegistry(t)
	reg.SeedSchema("widgets-value", 1, 1, protoSchema(widgetProto))
	d := newDeser(t, reg.client(t), VerifyDecode())
	b := frame(t, 1, []int{0}, &testpb.Widget{Name: "gear"})

	var got testpb.Widget
	if err := d.Decode(context.Background(), b, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.GetName() != "gear" {
		t.Errorf("name = %q, want %q", got.GetName(), "gear")
	}
}

func TestDecodeNewResolvesTheTypeFromTheSchema(t *testing.T) {
	t.Parallel()

	reg := newRegistry(t)
	reg.SeedSchema("widgets-value", 1, 1, protoSchema(widgetProto))
	d := newDeser(t, reg.client(t))

	for _, tc := range []struct {
		name  string
		index []int
		msg   proto.Message
	}{
		{"first top level message", []int{0}, &testpb.Widget{Name: "gear"}},
		{"second top level message", []int{1}, &testpb.Pizza{Slices: 8}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := d.DecodeNew(context.Background(), frame(t, 1, tc.index, tc.msg))
			if err != nil {
				t.Fatalf("DecodeNew: %v", err)
			}
			if !proto.Equal(got, tc.msg) {
				t.Errorf("decoded %v, want %v", got, tc.msg)
			}
		})
	}
}

func TestDecodeNewResolvesANestedMessage(t *testing.T) {
	t.Parallel()

	reg := newRegistry(t)
	reg.SeedSchema("outer-value", 1, 1, protoSchema(`syntax = "proto3";
package testpb;
message Outer {
  message Inner { string value = 1; }
  Inner inner = 1;
}`))
	d := newDeser(t, reg.client(t))

	want := &testpb.Outer_Inner{Value: "deep"}
	got, err := d.DecodeNew(context.Background(), frame(t, 1, []int{0, 0}, want))
	if err != nil {
		t.Fatalf("DecodeNew: %v", err)
	}
	if !proto.Equal(got, want) {
		t.Errorf("decoded %v, want %v", got, want)
	}
}

// A schema that references another must pull the reference in to compile,
// which exercises the core's reference resolution through this decoder.
func TestDecodeNewFollowsSchemaReferences(t *testing.T) {
	t.Parallel()

	reg := newRegistry(t)
	reg.SeedSchema("leaf.proto", 1, 1, protoSchema(`syntax = "proto3";
package testpb;
message Leaf { string id = 1; }`))
	reg.SeedSchema("mid.proto", 1, 2, protoSchema(`syntax = "proto3";
package testpb;
import "leaf.proto";
message Mid { Leaf leaf = 1; }`, sr.SchemaReference{Name: "leaf.proto", Subject: "leaf.proto", Version: 1}))
	reg.SeedSchema("chain-value", 1, 3, protoSchema(`syntax = "proto3";
package testpb;
import "mid.proto";
message Chain { Mid mid = 1; }`, sr.SchemaReference{Name: "mid.proto", Subject: "mid.proto", Version: 1}))

	d := newDeser(t, reg.client(t))
	want := &testpb.Chain{Mid: &testpb.Mid{Leaf: &testpb.Leaf{Id: "x"}}}
	got, err := d.DecodeNew(context.Background(), frame(t, 3, []int{0}, want))
	if err != nil {
		t.Fatalf("DecodeNew: %v", err)
	}
	if !proto.Equal(got, want) {
		t.Errorf("decoded %v, want %v", got, want)
	}
}

func TestDecodeNewCachesTheCompiledDescriptor(t *testing.T) {
	t.Parallel()

	reg := newRegistry(t)
	reg.SeedSchema("widgets-value", 1, 1, protoSchema(widgetProto))
	d := newDeser(t, reg.client(t))
	b := frame(t, 1, []int{0}, &testpb.Widget{Name: "gear"})

	if _, err := d.DecodeNew(context.Background(), b); err != nil {
		t.Fatalf("DecodeNew: %v", err)
	}
	if n := reg.total(); n == 0 {
		t.Fatal("the first DecodeNew made no observed requests; the request tally is not working")
	}
	reg.reset()

	for range 50 {
		if _, err := d.DecodeNew(context.Background(), b); err != nil {
			t.Fatalf("DecodeNew: %v", err)
		}
	}
	if n := reg.total(); n != 0 {
		t.Errorf("%d registry requests across 50 warm DecodeNew calls, want 0: %v", n, reg.counts)
	}
}

func TestDecodeNewCollapsesConcurrentColdMisses(t *testing.T) {
	t.Parallel()

	reg := newRegistry(t)
	reg.SeedSchema("widgets-value", 1, 1, protoSchema(widgetProto))
	d := newDeser(t, reg.client(t))
	b := frame(t, 1, []int{0}, &testpb.Widget{Name: "gear"})

	const goroutines = 32
	var wg sync.WaitGroup
	var failures atomic.Int64
	start := make(chan struct{})
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := d.DecodeNew(context.Background(), b); err != nil {
				failures.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if n := failures.Load(); n != 0 {
		t.Fatalf("%d of %d concurrent decodes failed", n, goroutines)
	}
	if n := reg.counts["GET /schemas/ids/1"]; n > 2 {
		t.Errorf("%d schema fetches across %d concurrent cold decodes, want 1", n, goroutines)
	}
}

func TestDecodeNewUsesAnExplicitTypeRegistry(t *testing.T) {
	t.Parallel()

	reg := newRegistry(t)
	reg.SeedSchema("widgets-value", 1, 1, protoSchema(widgetProto))

	types := new(protoregistry.Types)
	d := newDeser(t, reg.client(t), MessageTypes(types))
	if err := d.RegisterMessage(&testpb.Widget{}); err != nil {
		t.Fatalf("RegisterMessage: %v", err)
	}

	got, err := d.DecodeNew(context.Background(), frame(t, 1, []int{0}, &testpb.Widget{Name: "gear"}))
	if err != nil {
		t.Fatalf("DecodeNew: %v", err)
	}
	if got.(*testpb.Widget).GetName() != "gear" {
		t.Errorf("decoded %v", got)
	}
}

func TestDecodeNewRejectsANonProtobufSchema(t *testing.T) {
	t.Parallel()

	reg := newRegistry(t)
	reg.SeedSchema("widgets-value", 1, 1, sr.Schema{
		Type:   sr.TypeAvro,
		Schema: `{"type":"record","name":"Widget","fields":[]}`,
	})
	d := newDeser(t, reg.client(t))

	_, err := d.DecodeNew(context.Background(), frame(t, 1, []int{0}, &testpb.Widget{Name: "gear"}))
	if err == nil {
		t.Fatal("DecodeNew accepted an Avro schema")
	}
	if !strings.Contains(err.Error(), "not protobuf") {
		t.Errorf("error %q does not say the schema is not protobuf", err)
	}
}

func TestDecodeRejectsABadHeader(t *testing.T) {
	t.Parallel()

	reg := newRegistry(t)
	d := newDeser(t, reg.client(t))

	for _, tc := range []struct {
		name string
		b    []byte
	}{
		{"empty", nil},
		{"no magic byte", []byte{9, 0, 0, 0, 1, 0}},
		{"truncated id", []byte{0, 0, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got testpb.Widget
			if err := d.Decode(context.Background(), tc.b, &got); err == nil {
				t.Error("Decode accepted a malformed header")
			}
			if _, err := d.DecodeNew(context.Background(), tc.b); err == nil {
				t.Error("DecodeNew accepted a malformed header")
			}
		})
	}
}

func TestDeserializerRejectsNilArguments(t *testing.T) {
	t.Parallel()

	if _, err := NewDeserializer(nil); err == nil {
		t.Error("NewDeserializer accepted a nil client")
	}
	reg := newRegistry(t)
	d := newDeser(t, reg.client(t))
	if err := d.Decode(context.Background(), nil, nil); err == nil {
		t.Error("Decode accepted a nil message")
	}
	if err := d.RegisterMessage(nil); err == nil {
		t.Error("RegisterMessage accepted a nil message")
	}
}
