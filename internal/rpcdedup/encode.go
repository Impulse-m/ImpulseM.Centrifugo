package rpcdedup

import (
	"encoding/binary"
)

// encodedBufferSize is the size of the stack buffer which holds the length-prefixed parts hashed by ScopeOf and
// Fingerprint. A longer input is appended to the heap, and is hashed the same.
const encodedBufferSize = 512

// appendPartString appends the length of part, then part, so that a concatenation of parts is unambiguous.
func appendPartString(b []byte, part string) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(part)))
	return append(b, part...)
}

// appendPartBytes is appendPartString for a byte slice.
func appendPartBytes(b []byte, part []byte) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(part)))
	return append(b, part...)
}
