package proxy

import (
	"fmt"
)

// bytesCodec is a gRPC codec which passes message bytes through untouched. The registry forwards
// client payloads as they are, so it neither parses nor encodes them.
type bytesCodec struct{}

// Name returns the codec name. Registry targets are protobuf services, so the name is proto.
func (bytesCodec) Name() string {
	return "proto"
}

// Marshal returns the bytes of v, which is a []byte or a *[]byte.
func (bytesCodec) Marshal(v any) ([]byte, error) {
	switch msg := v.(type) {
	case []byte:
		return msg, nil
	case *[]byte:
		if msg == nil {
			return nil, nil
		}
		return *msg, nil
	}
	return nil, fmt.Errorf("bytes codec: cannot marshal %T", v)
}

// Unmarshal copies data into v, which must be a *[]byte. The copy is made because gRPC may reuse
// the buffer after the call.
func (bytesCodec) Unmarshal(data []byte, v any) error {
	msg, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("bytes codec: cannot unmarshal into %T", v)
	}
	*msg = append((*msg)[:0], data...)
	return nil
}
