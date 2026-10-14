package rpcdedup

import (
	"crypto/sha256"
	"encoding/binary"
)

// Fingerprint returns the fingerprint of a call: a hash of its method and data. A duplicate msg_id in the
// same scope with another fingerprint is a different call, and gets neither the in-flight call nor its
// reply. The method and the data are hashed as they are written, each after its length, so the payload is
// not copied and the call allocates nothing at any size.
func Fingerprint(method string, data []byte) [32]byte {
	hash := sha256.New()
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(method)))
	hash.Write(length[:])
	hash.Write([]byte(method))
	binary.BigEndian.PutUint32(length[:], uint32(len(data)))
	hash.Write(length[:])
	hash.Write(data)
	var out [32]byte
	hash.Sum(out[:0])
	return out
}
