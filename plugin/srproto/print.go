package srproto

import (
	"strings"

	"github.com/jhump/protoreflect/v2/protoprint"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// This file is the only thing in this module that depends on
// jhump/protoreflect, and printFile is the only function in it.
//
// Auto-registration has to send .proto source to the registry, but generated
// Go code embeds a serialized FileDescriptorProto rather than source, so the
// text has to be reconstructed from the descriptor. Nothing in
// google.golang.org/protobuf or bufbuild/protocompile prints a descriptor
// back to source; protoprint is the only maintained implementation.
func printFile(file protoreflect.FileDescriptor) (string, error) {
	printer := protoprint.Printer{OmitComments: protoprint.CommentsAll}
	var w strings.Builder
	if err := printer.PrintProtoFile(file, &w); err != nil {
		return "", err
	}
	return w.String(), nil
}
