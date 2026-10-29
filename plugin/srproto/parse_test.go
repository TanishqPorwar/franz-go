package srproto

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoregistry"

	// Linked so its descriptors are in protoregistry.GlobalFiles, which is
	// what the import-resolution tests below exercise.
	_ "github.com/twmb/franz-go/plugin/srproto/testpb"
)

// The well-known types ship with protocompile, so they resolve with nothing
// registered as a reference and nothing supplied by the caller.
func TestParseResolvesWellKnownTypes(t *testing.T) {
	for _, imp := range []string{
		"google/protobuf/timestamp.proto",
		"google/protobuf/any.proto",
		"google/protobuf/descriptor.proto",
		"google/protobuf/struct.proto",
		"google/protobuf/wrappers.proto",
	} {
		t.Run(imp, func(t *testing.T) {
			_, err := parseSchema(context.Background(),
				`syntax = "proto3"; import "`+imp+`"; message M { string name = 1; }`, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Anything whose generated Go package is linked into the binary resolves from
// protoregistry.GlobalFiles, without being a registered schema reference.
// This is what makes vendoring google/type and confluent protos unnecessary.
func TestParseResolvesLinkedFiles(t *testing.T) {
	if _, err := protoregistry.GlobalFiles.FindFileByPath("dep.proto"); err != nil {
		t.Skipf("dep.proto is not globally registered: %v", err)
	}
	fd, err := parseSchema(context.Background(),
		`syntax = "proto3"; package testpb; import "dep.proto"; message M { testpb.Dep d = 1; }`, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := fd.Messages().Get(0).FullName(); got != "testpb.M" {
		t.Fatalf("parsed %q", got)
	}
}

// An import that is neither a registry reference nor linked in must say what
// to do about it, not just that it is missing.
func TestParseUnresolvedImportIsActionable(t *testing.T) {
	_, err := parseSchema(context.Background(),
		`syntax = "proto3"; import "totally/made/up.proto"; message M { string name = 1; }`, nil, nil)
	if err == nil {
		t.Fatal("expected an error for an unresolvable import")
	}
	msg := err.Error()
	for _, want := range []string{"totally/made/up.proto", "blank-import", "Imports option"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error should mention %q: %v", want, msg)
		}
	}
}

// The Imports escape hatch covers files that are neither references nor
// linked in.
func TestParseUsesSuppliedImports(t *testing.T) {
	imports := map[string]string{
		"extra/thing.proto": `syntax = "proto3"; package extra; message Thing { string id = 1; }`,
	}
	fd, err := parseSchema(context.Background(),
		`syntax = "proto3"; import "extra/thing.proto"; message M { extra.Thing t = 1; }`, nil, imports)
	if err != nil {
		t.Fatal(err)
	}
	if n := fd.Messages().Len(); n != 1 {
		t.Fatalf("message types %d", n)
	}
}

// Registry references take priority over anything linked in, so a schema
// evolves with the registry rather than with the local build.
func TestParsePrefersReferencesOverLinkedFiles(t *testing.T) {
	deps := map[string]string{
		"dep.proto": `syntax = "proto3"; package testpb; message Dep { string id = 1; int64 extra = 2; }`,
	}
	fd, err := parseSchema(context.Background(),
		`syntax = "proto3"; package testpb; import "dep.proto"; message M { testpb.Dep d = 1; }`, deps, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The linked dep.proto has one field; the reference text above has two.
	dep := fd.Imports().Get(0).FileDescriptor
	if got := dep.Messages().Get(0).Fields().Len(); got != 2 {
		t.Fatalf("dep.proto resolved with %d fields, want the 2 from the reference text "+
			"(the registry reference must win over the linked descriptor)", got)
	}
}

// And a reference beats an entry supplied with the Imports option, because
// the registry is the authority on what the record was actually written with.
func TestParsePrefersReferencesOverSuppliedImports(t *testing.T) {
	deps := map[string]string{
		"thing.proto": `syntax = "proto3"; package extra; message Thing { string id = 1; int64 a = 2; int64 b = 3; }`,
	}
	imports := map[string]string{
		"thing.proto": `syntax = "proto3"; package extra; message Thing { string id = 1; }`,
	}
	fd, err := parseSchema(context.Background(),
		`syntax = "proto3"; import "thing.proto"; message M { extra.Thing t = 1; }`, deps, imports)
	if err != nil {
		t.Fatal(err)
	}
	if got := fd.Imports().Get(0).Messages().Get(0).Fields().Len(); got != 3 {
		t.Fatalf("thing.proto resolved with %d fields, want the 3 from the reference text", got)
	}
}

// The error for a bad index reports the whole path, not just the element that
// was out of range, which is what makes a nested miss debuggable.
func TestMessageDescriptorAtReportsTheIndexPath(t *testing.T) {
	fd, err := parseSchema(context.Background(), `syntax = "proto3";
message Outer {
  message Inner {}
}`, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := messageDescriptorAt(fd, []int{0, 0}); err != nil {
		t.Fatalf("Outer.Inner should resolve: %v", err)
	}

	_, err = messageDescriptorAt(fd, []int{0, 3})
	if err == nil {
		t.Fatal("expected an out of range error")
	}
	if !strings.Contains(err.Error(), "[0 3]") {
		t.Errorf("error %q does not report the full index path", err)
	}
}

func TestMessageDescriptorAtWalksNestedTypes(t *testing.T) {
	fd, err := parseSchema(context.Background(), `syntax = "proto3";
package testns;
message First {}
message Second {
  message Inner {
    message Deepest {}
  }
}`, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		index []int
		want  string
	}{
		{[]int{0}, "testns.First"},
		{[]int{1}, "testns.Second"},
		{[]int{1, 0}, "testns.Second.Inner"},
		{[]int{1, 0, 0}, "testns.Second.Inner.Deepest"},
	} {
		md, err := messageDescriptorAt(fd, tc.index)
		if err != nil {
			t.Fatalf("index %v: %v", tc.index, err)
		}
		if got := string(md.FullName()); got != tc.want {
			t.Errorf("index %v resolved to %q, want %q", tc.index, got, tc.want)
		}
	}

	if _, err := messageDescriptorAt(fd, nil); err == nil {
		t.Error("an empty index path should be rejected")
	}
}
