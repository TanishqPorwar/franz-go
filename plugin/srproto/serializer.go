package srproto

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/serde"
)

// Serializer encodes protobuf messages using the Confluent schema registry
// wire format. It is safe for concurrent use.
//
// By default it derives the .proto text for a message from its compiled-in
// descriptor, registers it along with every imported file as a schema
// reference, and caches the resulting id per topic and message type.
// [UseSchemaID] and [UseLatestVersion] skip derivation entirely and take the
// id from the registry instead.
type Serializer struct {
	core *serde.Serializer
}

// NewSerializer returns a Serializer that uses cl for schema registration and
// lookup.
func NewSerializer(cl *sr.Client, opts ...Opt) (*Serializer, error) {
	if cl == nil {
		return nil, errors.New("schema registry client is required")
	}
	cfg := newCfg(opts)
	r := serde.NewResolver(cl, cfg.serdeOpts...)
	enc := &encoder{
		builder: newSchemaBuilder(r, cfg.keepCodegenOptions),
		imports: cfg.imports,
	}
	core, err := serde.NewSerializer(r, enc)
	if err != nil {
		return nil, err
	}
	return &Serializer{core: core}, nil
}

// Encode serializes msg for topic using schema registry framing.
func (s *Serializer) Encode(ctx context.Context, topic string, msg proto.Message) ([]byte, error) {
	return s.AppendEncode(ctx, topic, msg, nil)
}

// AppendEncode appends the encoded record value to dst and returns the
// extended slice, so callers can reuse a buffer across records.
func (s *Serializer) AppendEncode(ctx context.Context, topic string, msg proto.Message, dst []byte) ([]byte, error) {
	if msg == nil {
		return nil, errors.New("nil protobuf message")
	}
	return s.core.AppendEncode(ctx, topic, msg, dst)
}

// encoder is the serde.Encoder half: everything protobuf-specific about
// turning a message into a schema and a payload.
type encoder struct {
	builder *schemaBuilder
	imports map[string]string
}

var (
	_ serde.Encoder  = (*encoder)(nil)
	_ serde.Verifier = (*encoder)(nil)
)

func descriptorOf(v any) (protoreflect.MessageDescriptor, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("cannot encode %T, want a proto.Message", v)
	}
	return msg.ProtoReflect().Descriptor(), nil
}

func (e *encoder) RecordName(v any) (string, error) {
	md, err := descriptorOf(v)
	if err != nil {
		return "", err
	}
	return string(md.FullName()), nil
}

func (e *encoder) Index(v any) ([]int, error) {
	md, err := descriptorOf(v)
	if err != nil {
		return nil, err
	}
	return messageIndexArray(md), nil
}

func (e *encoder) SchemaFor(ctx context.Context, v any) (sr.Schema, error) {
	md, err := descriptorOf(v)
	if err != nil {
		return sr.Schema{}, err
	}
	return e.builder.schemaForMessage(ctx, md)
}

func (e *encoder) AppendMarshal(dst []byte, v any) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("cannot encode %T, want a proto.Message", v)
	}
	return proto.MarshalOptions{}.MarshalAppend(dst, msg)
}

// Verify checks that a schema resolved from the registry really does contain
// the message being encoded, at the index derived locally.
//
// The core only calls this when it took an id from the registry without
// asking for schema text, which is the UseSchemaID and UseLatestVersion
// paths. There the message indexes come from the local descriptor while the
// schema comes from the registry, and nothing has established that the two
// agree. Without this, a pinned id for an unrelated schema produces records
// that decode as the wrong message rather than failing.
func (e *encoder) Verify(ctx context.Context, rs serde.ResolvedSchema, v any) error {
	md, err := descriptorOf(v)
	if err != nil {
		return err
	}
	if rs.Schema.Type != sr.TypeProtobuf {
		return fmt.Errorf("schema id %d has type %s, not protobuf", rs.ID, rs.Schema.Type)
	}

	recordName := md.FullName()
	index := messageIndexArray(md)

	fd, err := parseSchema(ctx, rs.Schema.Schema, rs.Deps, e.imports)
	if err != nil {
		return fmt.Errorf("unable to parse schema id %d to validate %q against it: %w", rs.ID, recordName, err)
	}
	at, err := messageDescriptorAt(fd, index)
	if err != nil {
		return fmt.Errorf("schema id %d has no message at index %v for %q: %w", rs.ID, index, recordName, err)
	}
	if got := at.FullName(); got != recordName {
		return fmt.Errorf("schema id %d has message %q at index %v, but encoding %q; "+
			"the pinned or latest schema does not match this message type", rs.ID, got, index, recordName)
	}
	return nil
}
