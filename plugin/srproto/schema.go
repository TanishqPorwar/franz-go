package srproto

import (
	"context"
	"strings"
	"sync"

	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/serde"
)

// schemaBuilder turns a compiled-in descriptor back into the .proto text the
// registry wants, registering each imported file as a schema reference.
//
// Generated Go code embeds a serialized FileDescriptorProto, not .proto
// source, so registering a schema means reconstructing the source. That is
// what printFile does, and it is the only reason this file exists.
type schemaBuilder struct {
	resolver    *serde.Resolver
	keepCodegen bool
	cache       sync.Map // file path -> sr.Schema
}

func newSchemaBuilder(r *serde.Resolver, keepCodegenOptions bool) *schemaBuilder {
	return &schemaBuilder{resolver: r, keepCodegen: keepCodegenOptions}
}

func (b *schemaBuilder) schemaForMessage(ctx context.Context, md protoreflect.MessageDescriptor) (sr.Schema, error) {
	return b.schemaForFile(ctx, md.ParentFile())
}

func (b *schemaBuilder) schemaForFile(ctx context.Context, file protoreflect.FileDescriptor) (sr.Schema, error) {
	if v, ok := b.cache.Load(file.Path()); ok {
		return v.(sr.Schema), nil
	}
	deps := make(map[string]string)
	if err := b.collectDeps(file, deps); err != nil {
		return sr.Schema{}, err
	}
	schema, err := b.buildSchema(ctx, file, deps)
	if err != nil {
		return sr.Schema{}, err
	}
	b.cache.Store(file.Path(), schema)
	return schema, nil
}

// collectDeps prints file and everything it imports, transitively, into deps
// keyed by file path.
func (b *schemaBuilder) collectDeps(file protoreflect.FileDescriptor, deps map[string]string) error {
	if _, ok := deps[file.Path()]; ok {
		return nil
	}
	text, err := b.print(file)
	if err != nil {
		return err
	}
	deps[file.Path()] = text

	imports := file.Imports()
	for i := range imports.Len() {
		imp := imports.Get(i)
		if ignoreFile(imp.Path()) {
			continue
		}
		if err := b.collectDeps(imp.FileDescriptor, deps); err != nil {
			return err
		}
	}
	return nil
}

func (b *schemaBuilder) print(file protoreflect.FileDescriptor) (string, error) {
	if !b.keepCodegen {
		stripped, err := stripCodegenOptions(file)
		if err != nil {
			return "", err
		}
		file = stripped
	}
	return printFile(file)
}

// stripCodegenOptions returns file with its per-language codegen options
// cleared. Those options say nothing about the wire format and differ for the
// same .proto across languages, so leaving them in makes the registered
// schema depend on which language registered it.
//
// This operates on the descriptor rather than the printed text: matching
// "option go_package = ..." lines in output is fragile, and clearing the
// fields is exactly as precise as the descriptor itself.
func stripCodegenOptions(file protoreflect.FileDescriptor) (protoreflect.FileDescriptor, error) {
	// ToFileDescriptorProto builds a fresh message each call, so mutating it
	// cannot affect the descriptor we were handed.
	fdp := protodesc.ToFileDescriptorProto(file)
	if fdp.GetOptions() == nil {
		return file, nil
	}

	o := fdp.Options
	o.GoPackage = nil
	o.JavaPackage = nil
	o.JavaOuterClassname = nil
	o.JavaMultipleFiles = nil
	o.JavaStringCheckUtf8 = nil //nolint:staticcheck // cleared precisely because it is a deprecated codegen option
	o.JavaGenericServices = nil
	o.CcGenericServices = nil
	o.CcEnableArenas = nil
	o.PyGenericServices = nil
	o.PhpNamespace = nil
	o.PhpMetadataNamespace = nil
	o.PhpClassPrefix = nil
	o.RubyPackage = nil
	o.CsharpNamespace = nil
	o.SwiftPrefix = nil
	o.ObjcClassPrefix = nil

	if isEmptyFileOptions(o) {
		fdp.Options = nil
	}

	// Rebuild against the files linked into this binary, which is where the
	// original came from, so the descriptor stays linked to its imports.
	return protodesc.NewFile(fdp, protoregistry.GlobalFiles)
}

// isEmptyFileOptions reports whether every field of o is unset, so that a file
// whose only options were codegen options prints with no options block at all
// rather than an empty one.
func isEmptyFileOptions(o *descriptorpb.FileOptions) bool {
	empty := true
	o.ProtoReflect().Range(func(protoreflect.FieldDescriptor, protoreflect.Value) bool {
		empty = false
		return false
	})
	return empty
}

func (b *schemaBuilder) buildSchema(ctx context.Context, file protoreflect.FileDescriptor, deps map[string]string) (sr.Schema, error) {
	refs, err := b.importReferences(ctx, file, deps)
	if err != nil {
		return sr.Schema{}, err
	}
	return sr.Schema{
		Schema:     deps[file.Path()],
		Type:       sr.TypeProtobuf,
		References: refs,
	}, nil
}

// importReferences registers each imported file as a schema reference, so the
// registry stores one subject per .proto rather than one flattened blob.
func (b *schemaBuilder) importReferences(ctx context.Context, file protoreflect.FileDescriptor, deps map[string]string) ([]sr.SchemaReference, error) {
	seen := make(map[string]struct{})
	var refs []sr.SchemaReference

	imports := file.Imports()
	for i := range imports.Len() {
		imp := imports.Get(i)
		name := imp.Path()
		if ignoreFile(name) {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}

		refSchema, err := b.buildSchema(ctx, imp.FileDescriptor, deps)
		if err != nil {
			return nil, err
		}
		ss, err := b.resolver.RegisterDependency(ctx, name, refSchema)
		if err != nil {
			return nil, err
		}
		refs = append(refs, sr.SchemaReference{
			Name:    name,
			Subject: ss.Subject,
			Version: ss.Version,
		})
	}
	return refs, nil
}

// ignoreFile reports imports that are not registered as references. Every
// schema registry client resolves these from its own runtime rather than from
// the registry, so registering them would only create subjects nobody reads.
func ignoreFile(name string) bool {
	return strings.HasPrefix(name, "confluent/") ||
		strings.HasPrefix(name, "google/protobuf/") ||
		strings.HasPrefix(name, "google/type/")
}

// messageIndexArray returns the Confluent message-index path to descriptor:
// its index within the file, followed by the index of each nested message on
// the way down. sr.ConfluentHeader encodes the common [0] case as a single
// zero byte.
func messageIndexArray(descriptor protoreflect.Descriptor) []int {
	return toMessageIndexes(descriptor, 0)
}

// toMessageIndexes walks up to the file, then fills the path back in on the
// way down. depth is only used to size the slice once at the top so the
// appends below do not reallocate.
func toMessageIndexes(descriptor protoreflect.Descriptor, depth int) []int {
	index := descriptor.Index()
	parent, nested := descriptor.Parent().(protoreflect.MessageDescriptor)
	if !nested {
		indexes := make([]int, 1, depth+1)
		indexes[0] = index
		return indexes
	}
	return append(toMessageIndexes(parent, depth+1), index)
}
