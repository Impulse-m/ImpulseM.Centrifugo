package accessrevoke

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/centrifugal/centrifuge"
)

// Key derives a 16-byte key from a user or session identifier. A canonical UUID string
// (8-4-4-4-12 hex digits) is parsed into its 16 bytes, any other non-empty string is hashed
// and the first 16 bytes of SHA-256 are used. Empty string gives no key.
func Key(value string) ([16]byte, bool) {
	var key [16]byte
	if value == "" {
		return key, false
	}
	if parsed, ok := parseUUID(value); ok {
		return parsed, true
	}
	sum := sha256.Sum256([]byte(value))
	copy(key[:], sum[:16])
	return key, true
}

// Identity builds centrifuge.TokenIdentity from connection token fields. Empty session gives
// zero Session, which disables session checks for the connection.
func Identity(session string, generation uint64, issuedAt int64) centrifuge.TokenIdentity {
	sessionKey, _ := Key(session)
	return centrifuge.TokenIdentity{
		Session:    sessionKey,
		Generation: generation,
		IssuedAt:   issuedAt,
	}
}

func parseUUID(value string) ([16]byte, bool) {
	var key [16]byte
	if len(value) != 36 {
		return key, false
	}
	for _, pos := range []int{8, 13, 18, 23} {
		if value[pos] != '-' {
			return key, false
		}
	}
	raw, err := hex.DecodeString(value[0:8] + value[9:13] + value[14:18] + value[19:23] + value[24:36])
	if err != nil {
		return key, false
	}
	copy(key[:], raw)
	return key, true
}
