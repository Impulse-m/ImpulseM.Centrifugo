package confighelpers

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func encodeTestPublicKeyPEM(t *testing.T, pubKey any) string {
	der, err := x509.MarshalPKIXPublicKey(pubKey)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

func TestMakeVerifierConfig_Ed25519PublicKeys(t *testing.T) {
	pubKey1, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	pubKey2, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	pem1 := encodeTestPublicKeyPEM(t, pubKey1)
	pem2 := encodeTestPublicKeyPEM(t, pubKey2)

	t.Run("keys parsed by kid", func(t *testing.T) {
		cfg, err := MakeVerifierConfig(configtypes.Token{
			Ed25519PublicKeys: configtypes.Ed25519PublicKeys{
				{KID: "k1", PublicKey: pem1},
				{KID: "k2", PublicKey: pem2},
			},
		})
		require.NoError(t, err)
		require.Len(t, cfg.Ed25519PublicKeys, 2)
		require.Equal(t, pubKey1, cfg.Ed25519PublicKeys["k1"])
		require.Equal(t, pubKey2, cfg.Ed25519PublicKeys["k2"])
	})

	t.Run("single key without kid", func(t *testing.T) {
		cfg, err := MakeVerifierConfig(configtypes.Token{
			Ed25519PublicKeys: configtypes.Ed25519PublicKeys{
				{PublicKey: pem1},
			},
		})
		require.NoError(t, err)
		require.Equal(t, pubKey1, cfg.Ed25519PublicKeys[""])
	})

	t.Run("no keys", func(t *testing.T) {
		cfg, err := MakeVerifierConfig(configtypes.Token{})
		require.NoError(t, err)
		require.Empty(t, cfg.Ed25519PublicKeys)
	})

	t.Run("invalid PEM reports index and kid", func(t *testing.T) {
		_, err := MakeVerifierConfig(configtypes.Token{
			Ed25519PublicKeys: configtypes.Ed25519PublicKeys{
				{KID: "k1", PublicKey: pem1},
				{KID: "k2", PublicKey: "not a pem"},
			},
		})
		require.ErrorContains(t, err, "index 1")
		require.ErrorContains(t, err, `kid "k2"`)
	})

	t.Run("non Ed25519 key rejected", func(t *testing.T) {
		ecdsaKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		_, err = MakeVerifierConfig(configtypes.Token{
			Ed25519PublicKeys: configtypes.Ed25519PublicKeys{
				{KID: "k1", PublicKey: encodeTestPublicKeyPEM(t, &ecdsaKey.PublicKey)},
			},
		})
		require.ErrorContains(t, err, "not a valid Ed25519 public key")
		require.ErrorContains(t, err, `kid "k1"`)
	})

	t.Run("duplicate kid rejected", func(t *testing.T) {
		_, err := MakeVerifierConfig(configtypes.Token{
			Ed25519PublicKeys: configtypes.Ed25519PublicKeys{
				{KID: "k1", PublicKey: pem1},
				{KID: "k1", PublicKey: pem2},
			},
		})
		require.ErrorContains(t, err, `duplicate Ed25519 public key kid "k1" at index 1`)
	})
}

func TestMakeVerifierConfigEd25519(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	der, err := x509.MarshalPKIXPublicKey(pub)
	require.NoError(t, err)
	cfg, err := MakeVerifierConfig(configtypes.Token{
		Ed25519PublicKey: string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})),
	})
	require.NoError(t, err)
	require.True(t, pub.Equal(cfg.Ed25519PublicKey))

	_, err = MakeVerifierConfig(configtypes.Token{Ed25519PublicKey: "not a key"})
	require.ErrorContains(t, err, "Ed25519")
}
