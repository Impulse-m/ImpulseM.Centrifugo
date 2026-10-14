package rpcdedup

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

// anonymousKind is the scope kind of an anonymous connection, which is scoped by its instance only.
const anonymousKind = "a"

// userTagLen is the number of hex characters of the user tag of a scope: the start of the hash of the user id.
const userTagLen = 16

// hashLen is the number of hex characters of the hash of a scope.
const hashLen = 2 * sha256.Size

// authenticatedScopeLen is the length of a scope of an authenticated user: kind, user tag and hash, separated by
// colons. The shape is "<kind>:<usertag>:<hash>".
const authenticatedScopeLen = 1 + 1 + userTagLen + 1 + hashLen

// anonymousScopeLen is the length of a scope of an anonymous connection: kind and hash. The shape is "a:<hash>".
const anonymousScopeLen = 1 + 1 + hashLen

// encodedBufferSize is the size of the stack buffer which holds the length-prefixed parts hashed by ScopeOf and
// Fingerprint. A longer input is appended to the heap, and is hashed the same.
const encodedBufferSize = 512

// ScopeOf returns the dedup scope of a connection. kind is "s" for a token session and "i" for an install
// instance, and userID is the user of the connection. id is the session or the instance. The anonymous kind "a"
// is scoped by its instance only, and its scope has no user tag. The parts are hashed over a length-prefixed
// encoding, so no user id or instance can make two scopes equal. The scope does not show id, so it may be logged.
func ScopeOf(kind string, userID string, id []byte) string {
	var stack [encodedBufferSize]byte
	encoded := appendPartString(stack[:0], kind)
	encoded = appendPartString(encoded, userID)
	encoded = appendPartBytes(encoded, id)
	digest := sha256.Sum256(encoded)

	var out [authenticatedScopeLen]byte
	n := copy(out[:], kind)
	out[n] = ':'
	n++
	if kind != anonymousKind {
		userDigest := sha256.Sum256(append(stack[:0], userID...))
		n += hex.Encode(out[n:], userDigest[:userTagLen/2])
		out[n] = ':'
		n++
	}
	n += hex.Encode(out[n:], digest[:])
	return string(out[:n])
}

// Fingerprint returns the fingerprint of a call: a hash of its method and data. A duplicate msg_id in the
// same scope with another fingerprint is a different call, and gets neither the in-flight call nor its
// reply.
func Fingerprint(method string, data []byte) [32]byte {
	var stack [encodedBufferSize]byte
	encoded := appendPartString(stack[:0], method)
	encoded = appendPartBytes(encoded, data)
	return sha256.Sum256(encoded)
}

// scopeUser returns the user tag of an authenticated scope, which is the shard key of its user. It returns the
// empty string for an anonymous scope, and for a scope of another shape.
func scopeUser(scope string) string {
	if len(scope) != authenticatedScopeLen {
		return ""
	}
	if scope[0] != 's' && scope[0] != 'i' {
		return ""
	}
	if scope[1] != ':' || scope[2+userTagLen] != ':' {
		return ""
	}
	if !isLowerHex(scope[2:2+userTagLen]) || !isLowerHex(scope[3+userTagLen:]) {
		return ""
	}
	return scope[2 : 2+userTagLen]
}

// scopeAnonymous reports whether scope is the scope of an anonymous connection.
func scopeAnonymous(scope string) bool {
	if len(scope) != anonymousScopeLen || scope[0] != 'a' || scope[1] != ':' {
		return false
	}
	return isLowerHex(scope[2:])
}

// isLowerHex reports whether s holds only lowercase hex digits, as hex.Encode writes them.
func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

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
