package srproto

import (
	"google.golang.org/protobuf/reflect/protoregistry"
)

// Opt configures a Deserializer.
type Opt interface {
	apply(*cfg)
}

type cfg struct {
	imports      map[string]string
	types        *protoregistry.Types
	verifyDecode bool
}

func newCfg(opts []Opt) cfg {
	var c cfg
	for _, o := range opts {
		o.apply(&c)
	}
	return c
}

type opt struct{ fn func(*cfg) }

func (o opt) apply(c *cfg) { o.fn(c) }

// Imports supplies .proto source text for imports that are neither registered
// schema references nor linked into this binary, keyed by the path used in the
// import statement.
//
// This is an escape hatch. Normally an import resolves from
// protoregistry.GlobalFiles because its generated Go package is linked in.
func Imports(files map[string]string) Opt {
	return opt{func(c *cfg) {
		if c.imports == nil {
			c.imports = make(map[string]string, len(files))
		}
		for k, v := range files {
			c.imports[k] = v
		}
	}}
}

// MessageTypes sets an additional registry that DecodeNew resolves concrete Go
// types from, consulted before protoregistry.GlobalTypes.
func MessageTypes(types *protoregistry.Types) Opt {
	return opt{func(c *cfg) { c.types = types }}
}

// VerifyDecode makes Decode check that the schema id and message indexes in
// the record header actually describe the message being decoded into, instead
// of unmarshaling blindly.
//
// Decode is otherwise the one path that needs no registry interaction at all,
// so this trades that for catching a producer that mislabeled its records. It
// costs one schema resolution per unseen id, cached thereafter.
func VerifyDecode() Opt {
	return opt{func(c *cfg) { c.verifyDecode = true }}
}
