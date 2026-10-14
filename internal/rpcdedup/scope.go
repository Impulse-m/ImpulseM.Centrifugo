package rpcdedup

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
)

// ScopeOf returns the dedup scope of a connection. kind is "s" for a token session and "i" for an install
// instance, userID is the user of the connection (empty when anonymous), and id is the session or the
// instance. The parts are hashed over a length-prefixed encoding, so no user id or instance can make two
// scopes equal. The scope does not show id, so it may be logged.
func ScopeOf(kind string, userID string, id []byte) string {
	h := sha256.New()
	writeLengthPrefixed(h, []byte(kind))
	writeLengthPrefixed(h, []byte(userID))
	writeLengthPrefixed(h, id)
	return kind + ":" + hex.EncodeToString(h.Sum(nil))
}

// Fingerprint returns the fingerprint of a call: a hash of its method and data. A duplicate msg_id in the
// same scope with another fingerprint is a different call, and gets neither the in-flight call nor its
// reply.
func Fingerprint(method string, data []byte) [32]byte {
	h := sha256.New()
	writeLengthPrefixed(h, []byte(method))
	writeLengthPrefixed(h, data)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// writeLengthPrefixed writes the length of part, then part, so that a concatenation of parts is unambiguous.
func writeLengthPrefixed(h hash.Hash, part []byte) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(part)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(part)
}
