// Package srproto decodes protobuf messages written in the Confluent schema
// registry wire format: a magic byte, the schema id, the message index path,
// and then the protobuf payload.
//
// It is an implementation of the interfaces in
// [github.com/twmb/franz-go/pkg/sr/serde], which owns subject naming, schema
// id resolution, and the wire framing. This package supplies the protobuf
// half: compiling schema text into descriptors and marshaling.
//
// # Imports
//
// Schema text is compiled with the protobuf files linked into this binary, by
// way of protoregistry.GlobalFiles, plus the well-known google/protobuf
// types. A schema that imports something else, such as google/type/latlng.proto
// or confluent/meta.proto, resolves only if the generated Go package for it is
// linked in. On the decode path this is automatic, because the message being
// decoded is registered and its generated file imports whatever its schema
// imports. Otherwise blank-import the package:
//
//	import _ "google.golang.org/genproto/googleapis/type/latlng"
//
// or pass the file's text with the [Imports] option.
package srproto

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/serde"
)

// Deserializer decodes Confluent schema-registry framed protobuf payloads. It
// is safe for concurrent use.
//
// Decode strips the wire header and unmarshals into the value the caller
// passes. By default it does not check that the schema id and message indexes
// describe that value, and so makes no registry request at all; pass
// [VerifyDecode] to make it check. DecodeNew always resolves the schema id and
// message indexes, because it has to pick the type.
type Deserializer struct {
	core *serde.Deserializer
	dec  *decoder
}

// NewDeserializer returns a Deserializer for cl.
func NewDeserializer(cl *sr.Client, opts ...Opt) (*Deserializer, error) {
	if cl == nil {
		return nil, errors.New("schema registry client is required")
	}
	cfg := newCfg(opts)
	if cfg.types == nil {
		cfg.types = new(protoregistry.Types)
	}
	dec := &decoder{
		types:   cfg.types,
		imports: cfg.imports,
		verify:  cfg.verifyDecode,
		files:   newLRU[int, protoreflect.FileDescriptor](maxCacheEntries),
	}
	core, err := serde.NewDeserializer(cl, dec)
	if err != nil {
		return nil, err
	}
	return &Deserializer{core: core, dec: dec}, nil
}

// RegisterMessage registers a generated protobuf message type for DecodeNew.
//
// Generated code already registers itself in protoregistry.GlobalTypes, which
// DecodeNew falls back to, so this is only needed for types that are not
// globally registered or to restrict which types may be decoded.
func (d *Deserializer) RegisterMessage(msg proto.Message) error {
	if msg == nil {
		return errors.New("nil protobuf message")
	}
	return d.dec.types.RegisterMessage(msg.ProtoReflect().Type())
}

// Decode unmarshals b into msg. The caller must pass the correct concrete
// type; the schema id and message indexes are not checked against msg unless
// the Deserializer was built with [VerifyDecode].
func (d *Deserializer) Decode(ctx context.Context, b []byte, msg proto.Message) error {
	if msg == nil {
		return errors.New("nil protobuf message")
	}
	return d.core.Decode(ctx, b, msg)
}

// DecodeNew unmarshals b into a new message instance looked up by schema id
// and message indexes. Types are resolved from any registry passed to
// [MessageTypes] and from types added with RegisterMessage, then from
// protoregistry.GlobalTypes.
func (d *Deserializer) DecodeNew(ctx context.Context, b []byte) (proto.Message, error) {
	v, err := d.core.DecodeNew(ctx, b)
	if err != nil {
		return nil, err
	}
	return v.(proto.Message), nil
}

// decoder is the serde.Decoder half: everything protobuf-specific about
// turning a framed record back into a message.
type decoder struct {
	types   *protoregistry.Types
	imports map[string]string
	verify  bool

	files  *lru[int, protoreflect.FileDescriptor]
	flight flight[int, protoreflect.FileDescriptor]
}

var _ serde.Decoder = (*decoder)(nil)

func (d *decoder) Unmarshal(ctx context.Context, w serde.WirePayload, s serde.SchemaLoader, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("cannot decode into %T, want a proto.Message", v)
	}
	if d.verify {
		md, err := d.messageDescriptor(ctx, w, s)
		if err != nil {
			return err
		}
		want := msg.ProtoReflect().Descriptor().FullName()
		if got := md.FullName(); got != want {
			return fmt.Errorf("record was encoded as %q (schema id %d, index %v) but is being decoded into %q", got, w.ID, w.Index, want)
		}
	}
	return proto.Unmarshal(w.Payload, msg)
}

func (d *decoder) UnmarshalNew(ctx context.Context, w serde.WirePayload, s serde.SchemaLoader) (any, error) {
	md, err := d.messageDescriptor(ctx, w, s)
	if err != nil {
		return nil, err
	}
	mt, err := d.findMessage(md.FullName())
	if err != nil {
		return nil, err
	}
	msg := mt.New().Interface()
	if err := proto.Unmarshal(w.Payload, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (d *decoder) findMessage(name protoreflect.FullName) (protoreflect.MessageType, error) {
	mt, err := d.types.FindMessageByName(name)
	if err == nil {
		return mt, nil
	}
	if !errors.Is(err, protoregistry.NotFound) {
		return nil, err
	}
	mt, err = protoregistry.GlobalTypes.FindMessageByName(name)
	if err != nil {
		if errors.Is(err, protoregistry.NotFound) {
			return nil, fmt.Errorf("message type %q is not registered: import the generated Go package for it, or add it with RegisterMessage", name)
		}
		return nil, err
	}
	return mt, nil
}

func (d *decoder) messageDescriptor(ctx context.Context, w serde.WirePayload, s serde.SchemaLoader) (protoreflect.MessageDescriptor, error) {
	fd, err := d.fileForID(ctx, w, s)
	if err != nil {
		return nil, err
	}
	return messageDescriptorAt(fd, w.Index)
}

// fileForID compiles the schema behind a value's id, caching the descriptor.
// The schema text itself is cached by the core; this caches the compilation,
// which is the expensive half.
func (d *decoder) fileForID(ctx context.Context, w serde.WirePayload, s serde.SchemaLoader) (protoreflect.FileDescriptor, error) {
	if fd, ok := d.files.get(w.ID); ok {
		return fd, nil
	}
	// Collapse concurrent misses for one id into a single compilation;
	// otherwise a cold cache under load compiles once per in-flight record.
	return d.flight.do(w.ID, func() (protoreflect.FileDescriptor, error) {
		if fd, ok := d.files.get(w.ID); ok {
			return fd, nil
		}
		rs, err := s.Load(ctx, w.ID)
		if err != nil {
			return nil, err
		}
		if rs.Schema.Type != sr.TypeProtobuf {
			return nil, fmt.Errorf("schema id %d has type %s, not protobuf", w.ID, rs.Schema.Type)
		}
		fd, err := parseSchema(ctx, rs.Schema.Schema, rs.Deps, d.imports)
		if err != nil {
			return nil, err
		}
		d.files.add(w.ID, fd)
		return fd, nil
	})
}
