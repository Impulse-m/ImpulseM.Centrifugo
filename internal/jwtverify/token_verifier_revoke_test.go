package jwtverify

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/cristalhq/jwt/v5"
	"github.com/stretchr/testify/require"
)

func getHMACConnTokenWithClaims(t *testing.T, claims map[string]any, secret string) string {
	t.Helper()
	signer, err := jwt.NewSignerHS(jwt.HS256, []byte(secret))
	require.NoError(t, err)
	token, err := jwt.NewBuilder(signer).Build(claims)
	require.NoError(t, err)
	return token.String()
}

func TestVerifyConnectTokenRevokeClaimsDefaults(t *testing.T) {
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	verifier, err := NewTokenVerifierJWT(VerifierConfig{HMACSecretKey: "secret"}, cfgContainer)
	require.NoError(t, err)

	token := getHMACConnTokenWithClaims(t, map[string]any{
		"sub": "user-1",
		"jti": "session-1",
		"iat": 1700000000,
		"gen": 7,
	}, "secret")
	ct, err := verifier.VerifyConnectToken(token, false)
	require.NoError(t, err)
	// Session is taken from jti by default, generation is off without generation claim.
	require.Equal(t, "session-1", ct.Session)
	require.Equal(t, uint64(0), ct.Generation)
	require.Equal(t, int64(1700000000), ct.IssuedAt)
}

func TestVerifyConnectTokenRevokeClaimsCustomNames(t *testing.T) {
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	verifier, err := NewTokenVerifierJWT(VerifierConfig{
		HMACSecretKey:   "secret",
		SessionClaim:    "sid",
		GenerationClaim: "gen",
	}, cfgContainer)
	require.NoError(t, err)

	token := getHMACConnTokenWithClaims(t, map[string]any{
		"sub": "user-1",
		"jti": "ignored-jti",
		"sid": "session-2",
		"gen": 7,
		"iat": 1700000100,
	}, "secret")
	ct, err := verifier.VerifyConnectToken(token, false)
	require.NoError(t, err)
	require.Equal(t, "session-2", ct.Session)
	require.Equal(t, uint64(7), ct.Generation)
	require.Equal(t, int64(1700000100), ct.IssuedAt)
}

func TestVerifyConnectTokenRevokeClaimsMissingOrInvalid(t *testing.T) {
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	verifier, err := NewTokenVerifierJWT(VerifierConfig{
		HMACSecretKey:   "secret",
		SessionClaim:    "sid",
		GenerationClaim: "gen",
	}, cfgContainer)
	require.NoError(t, err)

	tests := []struct {
		name       string
		claims     map[string]any
		generation uint64
	}{
		{name: "absent claims", claims: map[string]any{"sub": "user-1"}, generation: 0},
		{name: "string generation is ignored", claims: map[string]any{"sub": "user-1", "gen": "7"}, generation: 0},
		{name: "negative generation is ignored", claims: map[string]any{"sub": "user-1", "gen": -1}, generation: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token := getHMACConnTokenWithClaims(t, tt.claims, "secret")
			ct, err := verifier.VerifyConnectToken(token, false)
			require.NoError(t, err)
			require.Empty(t, ct.Session)
			require.Equal(t, tt.generation, ct.Generation)
			require.Equal(t, int64(0), ct.IssuedAt)
		})
	}
}

func TestVerifyConnectTokenRevokeClaimsOnReload(t *testing.T) {
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	verifier, err := NewTokenVerifierJWT(VerifierConfig{HMACSecretKey: "secret"}, cfgContainer)
	require.NoError(t, err)

	token := getHMACConnTokenWithClaims(t, map[string]any{
		"sub": "user-1",
		"jti": "jti-1",
		"sid": "session-3",
		"gen": 4,
	}, "secret")
	ct, err := verifier.VerifyConnectToken(token, false)
	require.NoError(t, err)
	require.Equal(t, "jti-1", ct.Session)
	require.Equal(t, uint64(0), ct.Generation)

	require.NoError(t, verifier.Reload(VerifierConfig{
		HMACSecretKey:   "secret",
		SessionClaim:    "sid",
		GenerationClaim: "gen",
	}))
	ct, err = verifier.VerifyConnectToken(token, false)
	require.NoError(t, err)
	require.Equal(t, "session-3", ct.Session)
	require.Equal(t, uint64(4), ct.Generation)
}
