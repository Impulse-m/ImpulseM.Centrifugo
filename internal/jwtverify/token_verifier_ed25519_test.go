package jwtverify

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/cristalhq/jwt/v5"
	"github.com/stretchr/testify/require"
)

func generateTestEd25519Keys(t *testing.T) (ed25519.PrivateKey, ed25519.PublicKey) {
	pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return privKey, pubKey
}

func getEd25519Token(t *testing.T, privKey ed25519.PrivateKey, claims any, opts ...jwt.BuilderOption) string {
	signer, err := jwt.NewSignerEdDSA(privKey)
	require.NoError(t, err)
	token, err := jwt.NewBuilder(signer, opts...).Build(claims)
	require.NoError(t, err)
	return token.String()
}

func getEd25519ConnToken(t *testing.T, user string, exp int64, privKey ed25519.PrivateKey, opts ...jwt.BuilderOption) string {
	claims := &ConnectTokenClaims{
		Base64Info: "e30=",
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: user,
		},
	}
	if exp > 0 {
		claims.ExpiresAt = jwt.NewNumericDate(time.Unix(exp, 0))
	}
	return getEd25519Token(t, privKey, claims, opts...)
}

func Test_tokenVerifierJWT_Ed25519(t *testing.T) {
	cfg := config.DefaultConfig()
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	exp := time.Now().Add(time.Hour).Unix()

	priv1, pub1 := generateTestEd25519Keys(t)
	priv2, pub2 := generateTestEd25519Keys(t)

	t.Run("known kid verifies", func(t *testing.T) {
		verifier, err := NewTokenVerifierJWT(VerifierConfig{
			Ed25519PublicKeys: map[string]ed25519.PublicKey{"k1": pub1, "k2": pub2},
		}, cfgContainer)
		require.NoError(t, err)

		token := getEd25519ConnToken(t, "user1", exp, priv2, jwt.WithKeyID("k2"))
		ct, err := verifier.VerifyConnectToken(token, false)
		require.NoError(t, err)
		require.Equal(t, "user1", ct.UserID)
	})

	t.Run("unknown kid rejected", func(t *testing.T) {
		verifier, err := NewTokenVerifierJWT(VerifierConfig{
			Ed25519PublicKeys: map[string]ed25519.PublicKey{"k1": pub1, "k2": pub2},
		}, cfgContainer)
		require.NoError(t, err)

		token := getEd25519ConnToken(t, "user1", exp, priv1, jwt.WithKeyID("unknown"))
		_, err = verifier.VerifyConnectToken(token, false)
		require.ErrorIs(t, err, ErrInvalidToken)
		require.ErrorContains(t, err, "unknown Ed25519 key ID")
	})

	t.Run("kid of another key rejected", func(t *testing.T) {
		verifier, err := NewTokenVerifierJWT(VerifierConfig{
			Ed25519PublicKeys: map[string]ed25519.PublicKey{"k1": pub1, "k2": pub2},
		}, cfgContainer)
		require.NoError(t, err)

		// Signed with priv2 but claims kid k1: must fail signature check, no fallback to other keys.
		token := getEd25519ConnToken(t, "user1", exp, priv2, jwt.WithKeyID("k1"))
		_, err = verifier.VerifyConnectToken(token, false)
		require.ErrorIs(t, err, ErrInvalidToken)
	})

	t.Run("no kid with one key accepted", func(t *testing.T) {
		verifier, err := NewTokenVerifierJWT(VerifierConfig{
			Ed25519PublicKeys: map[string]ed25519.PublicKey{"": pub1},
		}, cfgContainer)
		require.NoError(t, err)

		token := getEd25519ConnToken(t, "user1", exp, priv1)
		ct, err := verifier.VerifyConnectToken(token, false)
		require.NoError(t, err)
		require.Equal(t, "user1", ct.UserID)
	})

	t.Run("no kid with two keys rejected", func(t *testing.T) {
		verifier, err := NewTokenVerifierJWT(VerifierConfig{
			Ed25519PublicKeys: map[string]ed25519.PublicKey{"k1": pub1, "k2": pub2},
		}, cfgContainer)
		require.NoError(t, err)

		// Even when the token is signed with one of the configured keys, no kid means rejection.
		token := getEd25519ConnToken(t, "user1", exp, priv1)
		_, err = verifier.VerifyConnectToken(token, false)
		require.ErrorIs(t, err, ErrInvalidToken)
	})

	t.Run("no keys configured rejected as disabled algorithm", func(t *testing.T) {
		verifier, err := NewTokenVerifierJWT(VerifierConfig{HMACSecretKey: "secret"}, cfgContainer)
		require.NoError(t, err)

		token := getEd25519ConnToken(t, "user1", exp, priv1, jwt.WithKeyID("k1"))
		_, err = verifier.VerifyConnectToken(token, false)
		require.ErrorIs(t, err, ErrInvalidToken)
		require.ErrorContains(t, err, "disabled JWT algorithm")
	})

	t.Run("HS256 and EdDSA accepted together", func(t *testing.T) {
		verifier, err := NewTokenVerifierJWT(VerifierConfig{
			HMACSecretKey:     "secret",
			Ed25519PublicKeys: map[string]ed25519.PublicKey{"k1": pub1},
		}, cfgContainer)
		require.NoError(t, err)

		hmacToken := getHMACConnToken("user1", exp, "secret")
		ct, err := verifier.VerifyConnectToken(hmacToken, false)
		require.NoError(t, err)
		require.Equal(t, "user1", ct.UserID)

		eddsaToken := getEd25519ConnToken(t, "user2", exp, priv1, jwt.WithKeyID("k1"))
		ct, err = verifier.VerifyConnectToken(eddsaToken, false)
		require.NoError(t, err)
		require.Equal(t, "user2", ct.UserID)
	})

	t.Run("multi-value audience", func(t *testing.T) {
		verifier, err := NewTokenVerifierJWT(VerifierConfig{
			Audience:          "b",
			Ed25519PublicKeys: map[string]ed25519.PublicKey{"k1": pub1},
		}, cfgContainer)
		require.NoError(t, err)

		claims := &ConnectTokenClaims{
			RegisteredClaims: jwt.RegisteredClaims{
				Subject:  "user1",
				Audience: []string{"a", "b"},
			},
		}
		ct, err := verifier.VerifyConnectToken(getEd25519Token(t, priv1, claims, jwt.WithKeyID("k1")), false)
		require.NoError(t, err)
		require.Equal(t, "user1", ct.UserID)

		claims.Audience = []string{"a", "c"}
		_, err = verifier.VerifyConnectToken(getEd25519Token(t, priv1, claims, jwt.WithKeyID("k1")), false)
		require.ErrorIs(t, err, ErrInvalidToken)
	})

	t.Run("reload adds and removes keys", func(t *testing.T) {
		verifier, err := NewTokenVerifierJWT(VerifierConfig{
			Ed25519PublicKeys: map[string]ed25519.PublicKey{"k1": pub1},
		}, cfgContainer)
		require.NoError(t, err)

		err = verifier.Reload(VerifierConfig{
			Ed25519PublicKeys: map[string]ed25519.PublicKey{"k1": pub1, "k2": pub2},
		})
		require.NoError(t, err)
		_, err = verifier.VerifyConnectToken(getEd25519ConnToken(t, "user1", exp, priv2, jwt.WithKeyID("k2")), false)
		require.NoError(t, err)

		err = verifier.Reload(VerifierConfig{
			Ed25519PublicKeys: map[string]ed25519.PublicKey{"k2": pub2},
		})
		require.NoError(t, err)
		_, err = verifier.VerifyConnectToken(getEd25519ConnToken(t, "user1", exp, priv1, jwt.WithKeyID("k1")), false)
		require.ErrorIs(t, err, ErrInvalidToken)
		_, err = verifier.VerifyConnectToken(getEd25519ConnToken(t, "user1", exp, priv2, jwt.WithKeyID("k2")), false)
		require.NoError(t, err)

		err = verifier.Reload(VerifierConfig{})
		require.NoError(t, err)
		_, err = verifier.VerifyConnectToken(getEd25519ConnToken(t, "user1", exp, priv2, jwt.WithKeyID("k2")), false)
		require.ErrorIs(t, err, ErrInvalidToken)
		require.ErrorContains(t, err, "disabled JWT algorithm")
	})

	t.Run("JWKS endpoint conflicts with Ed25519 keys", func(t *testing.T) {
		_, err := NewTokenVerifierJWT(VerifierConfig{
			JWKSPublicEndpoint: "https://example.com/.well-known/jwks.json",
			Ed25519PublicKeys:  map[string]ed25519.PublicKey{"k1": pub1},
		}, cfgContainer)
		require.ErrorContains(t, err, "ed25519")
	})
}
