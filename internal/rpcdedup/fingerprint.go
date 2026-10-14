package rpcdedup

import (
	"crypto/sha256"
)

// Fingerprint returns the fingerprint of a call: a hash of its method and data. A duplicate msg_id in the
// same scope with another fingerprint is a different call, and gets neither the in-flight call nor its
// reply.
func Fingerprint(method string, data []byte) [32]byte {
	var stack [encodedBufferSize]byte
	encoded := appendPartString(stack[:0], method)
	encoded = appendPartBytes(encoded, data)
	return sha256.Sum256(encoded)
}
