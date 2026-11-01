module schema_registry_protobuf

go 1.27.0

require (
	github.com/twmb/franz-go v1.21.7
	github.com/twmb/franz-go/pkg/sr v1.8.0
	github.com/twmb/franz-go/plugin/srproto v0.0.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/bufbuild/protocompile v0.14.1 // indirect
	github.com/jhump/protoreflect/v2 v2.0.0 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.15.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
)

replace (
	github.com/twmb/franz-go => ../..
	github.com/twmb/franz-go/pkg/sr => ../../pkg/sr
	github.com/twmb/franz-go/plugin/srproto => ../../plugin/srproto
)
