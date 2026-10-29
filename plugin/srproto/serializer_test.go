package srproto

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/plugin/srproto/testpb"
)

func newSer(t *testing.T, cl *sr.Client, opts ...Opt) *Serializer {
	t.Helper()
	s, err := NewSerializer(cl, opts...)
	if err != nil {
		t.Fatalf("NewSerializer: %v", err)
	}
	return s
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cl := newRegistry(t).client(t)
	ser := newSer(t, cl)
	deser := newDeser(t, cl)

	msg := &testpb.Widget{Name: "hello"}
	b, err := ser.Encode(ctx, "topic", msg)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var out testpb.Widget
	if err := deser.Decode(ctx, b, &out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out.GetName() != "hello" {
		t.Errorf("name = %q, want %q", out.GetName(), "hello")
	}

	got, err := deser.DecodeNew(ctx, b)
	if err != nil {
		t.Fatalf("DecodeNew: %v", err)
	}
	if w, ok := got.(*testpb.Widget); !ok || w.GetName() != "hello" {
		t.Errorf("DecodeNew returned %+v", got)
	}
}

func TestEncodeReusesTheSchemaID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reg := newRegistry(t)
	ser := newSer(t, reg.client(t))

	if _, err := ser.Encode(ctx, "topic", &testpb.Widget{Name: "a"}); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	reg.reset()

	for range 50 {
		if _, err := ser.Encode(ctx, "topic", &testpb.Widget{Name: "b"}); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}
	if n := reg.total(); n != 0 {
		t.Errorf("%d registry requests across 50 warm encodes, want 0: %v", n, reg.counts)
	}
}

// AppendEncode must extend the caller's buffer rather than assume it is empty.
func TestAppendEncodeReusesTheBuffer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ser := newSer(t, newRegistry(t).client(t))

	want, err := ser.Encode(ctx, "topic", &testpb.Widget{Name: "reuse"})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	buf := make([]byte, 0, 512)
	for i := range 3 {
		buf, err = ser.AppendEncode(ctx, "topic", &testpb.Widget{Name: "reuse"}, buf[:0])
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		if string(buf) != string(want) {
			t.Fatalf("iteration %d: got %x, want %x", i, buf, want)
		}
	}

	prefix := []byte("keep me")
	got, err := ser.AppendEncode(ctx, "topic", &testpb.Widget{Name: "reuse"}, prefix)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	if string(got) != string(prefix)+string(want) {
		t.Errorf("got %q, want the existing prefix retained", got)
	}
}

func TestAutoRegisterOffRequiresAnExistingSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cl := newRegistry(t).client(t)

	lookup := newSer(t, cl, AutoRegister(false))
	if _, err := lookup.Encode(ctx, "topic", &testpb.Widget{Name: "x"}); err == nil {
		t.Fatal("encoding succeeded against an unregistered schema with AutoRegister(false)")
	}

	if _, err := newSer(t, cl).Encode(ctx, "topic", &testpb.Widget{Name: "x"}); err != nil {
		t.Fatalf("Encode with auto registration: %v", err)
	}
	if _, err := lookup.Encode(ctx, "topic", &testpb.Widget{Name: "y"}); err != nil {
		t.Fatalf("Encode after the schema exists: %v", err)
	}
}

func TestUseLatestVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cl := newRegistry(t).client(t)
	if _, err := newSer(t, cl).Encode(ctx, "topic", &testpb.Widget{Name: "v1"}); err != nil {
		t.Fatalf("seed Encode: %v", err)
	}

	latest := newSer(t, cl, UseLatestVersion(true), AutoRegister(false))
	b, err := latest.Encode(ctx, "topic", &testpb.Widget{Name: "v2"})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var hdr sr.ConfluentHeader
	id, _, err := hdr.DecodeID(b)
	if err != nil {
		t.Fatalf("DecodeID: %v", err)
	}
	if id != 1 {
		t.Errorf("id = %d, want 1", id)
	}
}

// Encoding against a pinned or latest schema that does not describe the
// message must fail, rather than stamping a message index that points at an
// unrelated type.
func TestRejectsASchemaThatDoesNotMatchTheMessage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reg := newRegistry(t)
	reg.SeedSchema("topic-value", 1, 77, protoSchema(
		`syntax = "proto3"; package other; message Unrelated { string q = 1; }`))
	cl := reg.client(t)

	for _, tc := range []struct {
		name string
		opt  Opt
	}{
		{"latest", UseLatestVersion(true)},
		{"pinned", UseSchemaID(77)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ser := newSer(t, cl, tc.opt, AutoRegister(false))
			_, err := ser.Encode(ctx, "topic", &testpb.Widget{Name: "x"})
			if err == nil {
				t.Fatal("encoded against a schema with no matching message")
			}
			if !strings.Contains(err.Error(), "testpb.Widget") {
				t.Errorf("error should name the message being encoded: %v", err)
			}
		})
	}
}

// A pinned id that does describe the message encodes fine, and is verified
// once rather than per record.
func TestUseSchemaIDVerifiesOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reg := newRegistry(t)
	cl := reg.client(t)
	if _, err := newSer(t, cl).Encode(ctx, "topic", &testpb.Widget{Name: "seed"}); err != nil {
		t.Fatalf("seed Encode: %v", err)
	}

	pinned := newSer(t, cl, UseSchemaID(1))
	if _, err := pinned.Encode(ctx, "topic", &testpb.Widget{Name: "x"}); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	reg.reset()

	for range 20 {
		if _, err := pinned.Encode(ctx, "topic", &testpb.Widget{Name: "x"}); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}
	if n := reg.total(); n != 0 {
		t.Errorf("%d registry requests across 20 warm pinned encodes, want 0: %v", n, reg.counts)
	}
}

func TestMessageIndexes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ser := newSer(t, newRegistry(t).client(t))

	for _, tc := range []struct {
		name string
		enc  func() ([]byte, error)
		want []int
	}{
		{"first top level message", func() ([]byte, error) {
			return ser.Encode(ctx, "t1", &testpb.Widget{Name: "w"})
		}, []int{0}},
		{"second top level message", func() ([]byte, error) {
			return ser.Encode(ctx, "t2", &testpb.Pizza{Slices: 8})
		}, []int{1}},
		{"nested message", func() ([]byte, error) {
			return ser.Encode(ctx, "t3", &testpb.Outer_Inner{Value: "in"})
		}, []int{0, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := tc.enc()
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			var hdr sr.ConfluentHeader
			_, rest, err := hdr.DecodeID(b)
			if err != nil {
				t.Fatalf("DecodeID: %v", err)
			}
			idx, _, err := hdr.DecodeIndex(rest, 0)
			if err != nil {
				t.Fatalf("DecodeIndex: %v", err)
			}
			if len(idx) != len(tc.want) {
				t.Fatalf("indexes %v, want %v", idx, tc.want)
			}
			for i := range idx {
				if idx[i] != tc.want[i] {
					t.Fatalf("indexes %v, want %v", idx, tc.want)
				}
			}
		})
	}
}

// An imported .proto becomes its own subject rather than being inlined, so
// the registry stores one schema per file as other clients expect.
func TestImportsBecomeSchemaReferences(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cl := newRegistry(t).client(t)
	ser := newSer(t, cl)
	b, err := ser.Encode(ctx, "topic", &testpb.WithDep{Dep: &testpb.Dep{Id: "d"}})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	subjects, err := cl.Subjects(ctx)
	if err != nil {
		t.Fatalf("Subjects: %v", err)
	}
	var hasDep, hasRoot bool
	for _, s := range subjects {
		switch s {
		case "dep.proto":
			hasDep = true
		case "topic-value":
			hasRoot = true
		}
	}
	if !hasDep || !hasRoot {
		t.Fatalf("subjects %v, want both dep.proto and topic-value", subjects)
	}

	var out testpb.WithDep
	if err := newDeser(t, cl).Decode(ctx, b, &out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out.GetDep().GetId() != "d" {
		t.Errorf("got %+v", &out)
	}
}

// A transitive import chain registers every file, and decodes back through
// the reference graph.
func TestTransitiveImportsRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cl := newRegistry(t).client(t)
	want := &testpb.Chain{Mid: &testpb.Mid{Leaf: &testpb.Leaf{Id: "x"}}}

	b, err := newSer(t, cl).Encode(ctx, "chain", want)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	subjects, err := cl.Subjects(ctx)
	if err != nil {
		t.Fatalf("Subjects: %v", err)
	}
	for _, want := range []string{"chain-value", "mid.proto", "leaf.proto"} {
		var found bool
		for _, s := range subjects {
			if s == want {
				found = true
			}
		}
		if !found {
			t.Errorf("subjects %v missing %q", subjects, want)
		}
	}

	got, err := newDeser(t, cl).DecodeNew(ctx, b)
	if err != nil {
		t.Fatalf("DecodeNew: %v", err)
	}
	if got.(*testpb.Chain).GetMid().GetLeaf().GetId() != "x" {
		t.Errorf("decoded %v", got)
	}
}

func TestReferenceSubjectNaming(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cl := newRegistry(t).client(t)
	ser := newSer(t, cl, ReferenceSubject(func(name string) string { return "protos/" + name }))
	if _, err := ser.Encode(ctx, "topic", &testpb.WithDep{Dep: &testpb.Dep{Id: "d"}}); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	subjects, err := cl.Subjects(ctx)
	if err != nil {
		t.Fatalf("Subjects: %v", err)
	}
	for _, s := range subjects {
		if s == "protos/dep.proto" {
			return
		}
	}
	t.Errorf("subjects %v missing protos/dep.proto", subjects)
}

func TestSubjectStrategies(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cl := newRegistry(t).client(t)

	for _, tc := range []struct {
		name  string
		opts  []Opt
		topic string
		want  string
	}{
		{"topic name", nil, "orders", "orders-value"},
		{"key", []Opt{SubjectPart(PartKey)}, "orders", "orders-key"},
		{"record name", []Opt{SubjectStrategy(RecordName)}, "ignored", "testpb.Widget"},
		{"topic record name", []Opt{SubjectStrategy(TopicRecordName)}, "orders", "orders-testpb.Widget"},
	} {
		if _, err := newSer(t, cl, tc.opts...).Encode(ctx, tc.topic, &testpb.Widget{Name: "x"}); err != nil {
			t.Fatalf("%s: Encode: %v", tc.name, err)
		}
	}

	subjects, err := cl.Subjects(ctx)
	if err != nil {
		t.Fatalf("Subjects: %v", err)
	}
	have := make(map[string]bool, len(subjects))
	for _, s := range subjects {
		have[s] = true
	}
	for _, want := range []string{"orders-value", "orders-key", "testpb.Widget", "orders-testpb.Widget"} {
		if !have[want] {
			t.Errorf("subjects %v missing %q", subjects, want)
		}
	}
}

// A record-name subject must round trip: the deserializer resolves by schema
// id, so the subject strategy used to produce it is irrelevant to decoding.
func TestRecordNameSubjectRoundTrips(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cl := newRegistry(t).client(t)
	b, err := newSer(t, cl, SubjectStrategy(RecordName)).Encode(ctx, "ignored", &testpb.Widget{Name: "rn"})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := newDeser(t, cl).DecodeNew(ctx, b)
	if err != nil {
		t.Fatalf("DecodeNew: %v", err)
	}
	if w, ok := got.(*testpb.Widget); !ok || w.GetName() != "rn" {
		t.Errorf("got %+v", got)
	}
}

func TestCodegenOptionStripping(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("stripped by default", func(t *testing.T) {
		cl := newRegistry(t).client(t)
		if _, err := newSer(t, cl).Encode(ctx, "topic", &testpb.Widget{Name: "w"}); err != nil {
			t.Fatalf("Encode: %v", err)
		}
		ss, err := cl.SchemaByVersion(ctx, "topic-value", 1)
		if err != nil {
			t.Fatalf("SchemaByVersion: %v", err)
		}
		for _, opt := range []string{"go_package", "java_package", "objc_class_prefix"} {
			if strings.Contains(ss.Schema.Schema, opt) {
				t.Errorf("schema text contains %s:\n%s", opt, ss.Schema.Schema)
			}
		}
	})

	// KeepCodegenOptions exists so a schema can match what
	// confluent-kafka-go registers, which does not strip them.
	t.Run("kept on request", func(t *testing.T) {
		cl := newRegistry(t).client(t)
		if _, err := newSer(t, cl, KeepCodegenOptions()).Encode(ctx, "topic", &testpb.Widget{Name: "w"}); err != nil {
			t.Fatalf("Encode: %v", err)
		}
		ss, err := cl.SchemaByVersion(ctx, "topic-value", 1)
		if err != nil {
			t.Fatalf("SchemaByVersion: %v", err)
		}
		if !strings.Contains(ss.Schema.Schema, "go_package") {
			t.Errorf("schema text should retain go_package:\n%s", ss.Schema.Schema)
		}
	})
}

// Stripped schema text must still compile, which is what makes it safe to
// register: a consumer gets this text back and has to parse it.
func TestStrippedSchemaStillCompiles(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cl := newRegistry(t).client(t)
	b, err := newSer(t, cl).Encode(ctx, "topic", &testpb.WithDep{Dep: &testpb.Dep{Id: "d"}})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := newDeser(t, cl).DecodeNew(ctx, b)
	if err != nil {
		t.Fatalf("DecodeNew over registered schema text: %v", err)
	}
	if got.(*testpb.WithDep).GetDep().GetId() != "d" {
		t.Errorf("decoded %v", got)
	}
}

func TestConcurrentEncode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ser := newSer(t, newRegistry(t).client(t))

	const goroutines = 16
	var wg sync.WaitGroup
	var failures atomic.Int64
	start := make(chan struct{})
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 20 {
				if _, err := ser.Encode(ctx, "topic", &testpb.Widget{Name: "c"}); err != nil {
					failures.Add(1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	if n := failures.Load(); n != 0 {
		t.Errorf("%d concurrent encodes failed", n)
	}
}

func TestSerializerRejectsNilArguments(t *testing.T) {
	t.Parallel()

	if _, err := NewSerializer(nil); err == nil {
		t.Error("NewSerializer accepted a nil client")
	}
	ser := newSer(t, newRegistry(t).client(t))
	if _, err := ser.Encode(context.Background(), "topic", nil); err == nil {
		t.Error("Encode accepted a nil message")
	}
}
