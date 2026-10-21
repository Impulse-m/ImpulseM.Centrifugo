package jwtverify

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/stretchr/testify/require"
)

func TestVerifyConnectTokenTokenExp(t *testing.T) {
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	verifier, err := NewTokenVerifierJWT(VerifierConfig{HMACSecretKey: "secret"}, cfgContainer)
	require.NoError(t, err)

	const exp = int64(4102444800)

	// The exp claim is kept apart from expire_at, which decides the expiry of the connection.
	ct, err := verifier.VerifyConnectToken(getHMACTokenWithClaims(map[string]any{"sub": "42", "exp": exp, "expire_at": exp + 600}), false)
	require.NoError(t, err)
	require.Equal(t, exp, ct.TokenExp)
	require.Equal(t, exp+600, ct.ExpireAt)

	// Without expire_at, the connection expires at exp.
	ct, err = verifier.VerifyConnectToken(getHMACTokenWithClaims(map[string]any{"sub": "42", "exp": exp}), false)
	require.NoError(t, err)
	require.Equal(t, exp, ct.TokenExp)
	require.Equal(t, exp, ct.ExpireAt)

	// Without exp, the token has no JWT expiry.
	ct, err = verifier.VerifyConnectToken(getHMACTokenWithClaims(map[string]any{"sub": "42", "expire_at": exp}), false)
	require.NoError(t, err)
	require.Zero(t, ct.TokenExp)
	require.Equal(t, exp, ct.ExpireAt)
}
