package accessrevoke

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

const testUUID = "5e1d6c2a-4f3b-4a8e-9c7d-0b1a2c3d4e5f"

func TestKeyFromUUID(t *testing.T) {
	expected, err := hex.DecodeString("5e1d6c2a4f3b4a8e9c7d0b1a2c3d4e5f")
	require.NoError(t, err)

	key, ok := Key(testUUID)
	require.True(t, ok)
	require.Equal(t, expected, key[:])

	upperKey, ok := Key("5E1D6C2A-4F3B-4A8E-9C7D-0B1A2C3D4E5F")
	require.True(t, ok)
	require.Equal(t, key, upperKey)
}

func TestKeyFromNonUUIDIsHashed(t *testing.T) {
	sum := sha256.Sum256([]byte("user-42"))

	key, ok := Key("user-42")
	require.True(t, ok)
	require.Equal(t, sum[:16], key[:])
}

func TestKeyNotCanonicalUUIDIsHashed(t *testing.T) {
	// Braces and missing hyphens are not canonical UUID strings.
	for _, value := range []string{
		"{5e1d6c2a-4f3b-4a8e-9c7d-0b1a2c3d4e5f}",
		"5e1d6c2a4f3b4a8e9c7d0b1a2c3d4e5f",
		"5e1d6c2a-4f3b-4a8e-9c7d-0b1a2c3d4e5",  // too short
		"5e1d6c2a-4f3b-4a8e-9c7dx0b1a2c3d4e5f", // wrong separator
		"zzzzzzzz-4f3b-4a8e-9c7d-0b1a2c3d4e5f", // not hex
	} {
		sum := sha256.Sum256([]byte(value))
		key, ok := Key(value)
		require.True(t, ok, value)
		require.Equal(t, sum[:16], key[:], value)
	}
}

func TestKeyEmptyGivesNoKey(t *testing.T) {
	key, ok := Key("")
	require.False(t, ok)
	require.Equal(t, [16]byte{}, key)
}

func TestIdentityEmptySessionGivesZeroSession(t *testing.T) {
	identity := Identity("", 3, 100)
	require.Equal(t, [16]byte{}, identity.Session)
	require.Equal(t, uint64(3), identity.Generation)
	require.Equal(t, int64(100), identity.IssuedAt)

	identity = Identity(testUUID, 0, 0)
	sessionKey, _ := Key(testUUID)
	require.Equal(t, sessionKey, identity.Session)
}
