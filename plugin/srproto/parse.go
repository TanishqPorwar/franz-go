package srproto

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// rootFileName is the synthetic name the schema under compilation is given.
// Schema text fetched from the registry has no file name of its own.
const rootFileName = "schema.proto"

// unresolvedImport reports an import that is in neither the schema's registry
// references nor this binary, and names the fix rather than just the symptom.
//
// It is what the source resolver returns for a file it does not have. A
// CompositeResolver reports the first resolver's error when every resolver
// fails, and the source resolver is first, so this is what surfaces when an
// import cannot be found anywhere.
type unresolvedImport struct{ name string }

func (e *unresolvedImport) Error() string {
	return fmt.Sprintf("no schema for import %q: it is not a registered reference of this schema "+
		"and no generated Go package providing it is linked into this binary; either blank-import "+
		"the generated package (for google/type/latlng.proto that is "+
		`_ "google.golang.org/genproto/googleapis/type/latlng") or supply the file with the `+
		"Imports option", e.name)
}

// parseSchema compiles schema text into a file descriptor.
//
// Imports resolve in the order the resolvers are listed: deps (schemas the
// registry supplied as references), then imports (files supplied with the
// Imports option), then the protobuf files linked into this binary, then the
// well-known google/protobuf types protocompile ships with.
//
// deps is owned by the caller's cache and is only read here.
func parseSchema(ctx context.Context, schema string, deps, imports map[string]string) (protoreflect.FileDescriptor, error) {
	source := protocompile.ResolverFunc(func(name string) (protocompile.SearchResult, error) {
		switch {
		case name == rootFileName:
			return protocompile.SearchResult{Source: strings.NewReader(schema)}, nil
		case deps[name] != "":
			return protocompile.SearchResult{Source: strings.NewReader(deps[name])}, nil
		case imports[name] != "":
			return protocompile.SearchResult{Source: strings.NewReader(imports[name])}, nil
		}
		return protocompile.SearchResult{}, &unresolvedImport{name: name}
	})

	// Resolve anything the registry did not supply against the files compiled
	// into this binary. Generated Go code registers its descriptors in
	// protoregistry.GlobalFiles at init, so a schema importing
	// google/type/latlng.proto or confluent/meta.proto resolves as long as the
	// generated package is linked in. On the decode path it always is: the
	// message being decoded must be registered, and its generated file imports
	// everything its own schema imports.
	linked := protocompile.ResolverFunc(func(name string) (protocompile.SearchResult, error) {
		fd, err := protoregistry.GlobalFiles.FindFileByPath(name)
		if err != nil {
			return protocompile.SearchResult{}, err
		}
		return protocompile.SearchResult{Desc: fd}, nil
	})

	c := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(protocompile.CompositeResolver{source, linked}),
	}
	files, err := c.Compile(ctx, rootFileName)
	if err != nil {
		// Surface our own message out of protocompile's position wrapping.
		var u *unresolvedImport
		if errors.As(err, &u) {
			return nil, u
		}
		return nil, err
	}
	return files[0], nil
}

// messageDescriptorAt walks the Confluent message-index path: the first index
// selects from the file's top-level messages, and each one after that selects
// from the nested types of the message before it.
func messageDescriptorAt(fd protoreflect.FileDescriptor, indexes []int) (protoreflect.MessageDescriptor, error) {
	if len(indexes) == 0 {
		return nil, errors.New("empty message indexes")
	}
	var cur protoreflect.MessageDescriptor
	msgs := fd.Messages()
	for _, idx := range indexes {
		if idx < 0 || idx >= msgs.Len() {
			return nil, fmt.Errorf("message index %v is out of range for the schema", indexes)
		}
		cur = msgs.Get(idx)
		msgs = cur.Messages()
	}
	return cur, nil
}
