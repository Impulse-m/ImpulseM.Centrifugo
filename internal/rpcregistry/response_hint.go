package rpcregistry

import (
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// maxResponseHintTags bounds the top-level fields a response size hint scan reads. A request which names its size
// after this many fields gets the full response bound.
const maxResponseHintTags int = 256

// resolveResponseSizeHint returns a copy of hint with the default overhead applied, or nil when hint is nil.
func resolveResponseSizeHint(hint *configtypes.RPCResponseSizeHint) *configtypes.RPCResponseSizeHint {
	if hint == nil {
		return nil
	}
	resolved := *hint
	if resolved.OverheadBytes == 0 {
		resolved.OverheadBytes = configtypes.DefaultResponseSizeOverheadBytes
	}
	return &resolved
}

// ResponseBound returns the response bound of a call whose request is data. It is MaxResponseBytes, or, when the
// method has a response size hint and the request carries it, the hinted size plus the overhead. A hint which is
// zero, absent, malformed or at or over MaxResponseBytes minus the overhead gives MaxResponseBytes.
func (m *Method) ResponseBound(data []byte) int {
	hint := m.ResponseSizeHint
	if hint == nil {
		return m.MaxResponseBytes
	}
	size, found := responseSizeHint(data, hint.RequestField)
	if !found || size == 0 {
		return m.MaxResponseBytes
	}
	return hintedResponseBound(size, m.MaxResponseBytes, hint.OverheadBytes)
}

// hintedResponseBound returns size plus overhead, or max when that is not below max. The comparison is made before
// the addition, so a size near 2^64 cannot wrap into a small bound.
func hintedResponseBound(size uint64, maxBytes int, overhead int) int {
	if overhead >= maxBytes {
		return maxBytes
	}
	limit := uint64(maxBytes - overhead)
	if size >= limit {
		return maxBytes
	}
	return int(size) + overhead
}

// responseSizeHint returns the value of the first top-level field number of data, which must be a varint field.
// It reports false when the field is absent, is not a varint, or the message does not parse before it is found.
// A field of another length is skipped by its length, so the cost is the number of fields before the hinted one.
func responseSizeHint(data []byte, field int) (uint64, bool) {
	number := protowire.Number(field)
	for tags := 0; len(data) > 0; tags++ {
		if tags == maxResponseHintTags {
			return 0, false
		}
		fieldNumber, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return 0, false
		}
		data = data[n:]
		if fieldNumber == number {
			if wireType != protowire.VarintType {
				return 0, false
			}
			value, valueLen := protowire.ConsumeVarint(data)
			if valueLen < 0 {
				return 0, false
			}
			return value, true
		}
		n = protowire.ConsumeFieldValue(fieldNumber, wireType, data)
		if n < 0 {
			return 0, false
		}
		data = data[n:]
	}
	return 0, false
}
