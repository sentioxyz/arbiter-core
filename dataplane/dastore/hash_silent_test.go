package dastore

import (
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// The read path is hash-silent by contract: no fetch/stat response may
// carry a server-asserted content hash or length the client could trust.
func TestReadPathMessagesCarryNoHashAssertions(t *testing.T) {
	for _, m := range []protoreflect.Message{
		(&pb.FetchBegin{}).ProtoReflect(),
		(&pb.FetchData{}).ProtoReflect(),
		(&pb.FetchEnd{}).ProtoReflect(),
		(&pb.PayloadStat{}).ProtoReflect(),
	} {
		fields := m.Descriptor().Fields()
		for i := 0; i < fields.Len(); i++ {
			name := string(fields.Get(i).Name())
			if strings.Contains(name, "hash") || name == "payload_length" {
				t.Errorf("%s.%s breaks the hash-silent read path", m.Descriptor().Name(), name)
			}
		}
	}
}
