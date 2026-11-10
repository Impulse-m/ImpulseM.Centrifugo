package vapidkey

import (
	"crypto/ecdh"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
)

// newPair generates a VAPID key pair and returns both keys in unpadded base64url form.
func newPair(t *testing.T) (string, string) {
	t.Helper()
	private, err := ecdh.P256().GenerateKey(rand.Reader)
	require.NoError(t, err)
	return base64.RawURLEncoding.EncodeToString(private.PublicKey().Bytes()),
		base64.RawURLEncoding.EncodeToString(private.Bytes())
}

// A generated pair parses, and the returned key is a P-256 key.
func TestParseGeneratedPair(t *testing.T) {
	public, private := newPair(t)
	key, err := Parse(public, private)
	require.NoError(t, err)
	require.Equal(t, elliptic.P256(), key.Curve)
	require.NoError(t, CheckPublic(public))
}

// Padded base64url is accepted for both keys: 65 bytes need one "=" and 32 bytes need two.
func TestParsePaddingAccepted(t *testing.T) {
	private, err := ecdh.P256().GenerateKey(rand.Reader)
	require.NoError(t, err)
	public := base64.URLEncoding.EncodeToString(private.PublicKey().Bytes())
	secret := base64.URLEncoding.EncodeToString(private.Bytes())
	require.Contains(t, public, "=")
	require.Contains(t, secret, "=")

	_, err = Parse(public, secret)
	require.NoError(t, err)
	require.NoError(t, CheckPublic(public))
}

// Each refusal names what is wrong, and no error contains the key material.
func TestParseRefusals(t *testing.T) {
	public, private := newPair(t)
	_, otherPrivate := newPair(t)
	otherPublic, _ := newPair(t)

	zeroScalar := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	shortScalar := base64.RawURLEncoding.EncodeToString(make([]byte, 31))
	compressed := base64.RawURLEncoding.EncodeToString(append([]byte{0x02}, make([]byte, 32)...))
	offCurve := base64.RawURLEncoding.EncodeToString(append([]byte{0x04}, make([]byte, 64)...))
	shortPublic := base64.RawURLEncoding.EncodeToString(make([]byte, 32))

	tests := []struct {
		name    string
		public  string
		private string
		wantErr string
	}{
		{
			name:    "public key is not base64url",
			public:  "!!!",
			private: private,
			wantErr: "public key is not base64url",
		},
		{
			name:    "public key is too short",
			public:  shortPublic,
			private: private,
			wantErr: "public key is not a P-256 point",
		},
		{
			name:    "public key is compressed",
			public:  compressed,
			private: private,
			wantErr: "public key is not a P-256 point",
		},
		{
			name:    "public key is not on the curve",
			public:  offCurve,
			private: private,
			wantErr: "public key is not a P-256 point",
		},
		{
			name:    "private key is not base64url",
			public:  public,
			private: "!!!",
			wantErr: "private key is not base64url",
		},
		{
			name:    "private key is too short",
			public:  public,
			private: shortScalar,
			wantErr: "private key is not a P-256 scalar",
		},
		{
			name:    "private key is zero",
			public:  public,
			private: zeroScalar,
			wantErr: "private key is not a P-256 scalar",
		},
		{
			name:    "private key does not match the public key",
			public:  otherPublic,
			private: private,
			wantErr: "private key does not match the public key",
		},
		{
			name:    "private key of another pair",
			public:  public,
			private: otherPrivate,
			wantErr: "private key does not match the public key",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse(test.public, test.private)
			require.ErrorContains(t, err, test.wantErr)
			require.NotContains(t, err.Error(), test.private)
			require.NotContains(t, err.Error(), test.public)
		})
	}
}

// CheckPublic accepts a generated public key and refuses the wrong forms of one.
func TestCheckPublic(t *testing.T) {
	public, _ := newPair(t)
	require.NoError(t, CheckPublic(public))
	require.ErrorContains(t, CheckPublic("!!!"), "public key is not base64url")
	require.ErrorContains(t, CheckPublic(base64.RawURLEncoding.EncodeToString([]byte{0x04})), "public key is not a P-256 point")
}
