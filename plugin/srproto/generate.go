package srproto

//go:generate protoc --go_out=. --go_opt=module=github.com/twmb/franz-go/plugin/srproto -I testdata testdata/widget.proto testdata/nested.proto testdata/dep.proto testdata/with_dep.proto testdata/leaf.proto testdata/mid.proto testdata/chain.proto
