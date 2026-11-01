module github.com/twmb/franz-go/plugin/srproto

go 1.26.0

// RELEASE ORDER: this module needs the serde package, which first appears in
// pkg/sr v1.9.0. Until that tag exists the require below resolves to a pkg/sr
// without it, so CI supplies pkg/sr from the tree with a go.work. Bump the
// require and drop that workspace step once pkg/sr/v1.9.0 is tagged.
require (
	github.com/bufbuild/protocompile v0.14.1
	github.com/twmb/franz-go/pkg/sr v1.8.0
	google.golang.org/protobuf v1.36.6
)

require golang.org/x/sync v0.16.0 // indirect
