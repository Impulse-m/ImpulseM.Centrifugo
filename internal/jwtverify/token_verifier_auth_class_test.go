package jwtverify

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/stretchr/testify/require"
)

func getHMACTokenWithClaims(claims map[string]any) string {
	token, err := getRSATokenBuilder(nil).Build(claims)
	if err != nil {
		panic(err)
	}
	return token.String()
}

func Test_tokenVerifierJWT_AuthClassClaim(t *testing.T) {
	cfg := config.DefaultConfig()
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)

	verifier, err := NewTokenVerifierJWT(VerifierConfig{HMACSecretKey: "secret", AuthClassClaim: "auth_class"}, cfgContainer)
	require.NoError(t, err)

	// Absent claim gives an empty auth class.
	ct, err := verifier.VerifyConnectToken(getHMACTokenWithClaims(map[string]any{"sub": "42"}), false)
	require.NoError(t, err)
	require.Equal(t, "", ct.AuthClass)

	// String claim is taken as is, the verifier does not interpret it.
	ct, err = verifier.VerifyConnectToken(getHMACTokenWithClaims(map[string]any{"sub": "42", "auth_class": "pending_2fa"}), false)
	require.NoError(t, err)
	require.Equal(t, "pending_2fa", ct.AuthClass)

	// Claim which is not a string is an invalid token.
	for _, value := range []any{true, 1, []string{"bot"}, nil} {
		_, err = verifier.VerifyConnectToken(getHMACTokenWithClaims(map[string]any{"sub": "42", "auth_class": value}), false)
		require.ErrorIs(t, err, ErrInvalidToken)
	}

	// Without the claim configured, the claim is not read at all.
	verifier, err = NewTokenVerifierJWT(VerifierConfig{HMACSecretKey: "secret"}, cfgContainer)
	require.NoError(t, err)
	ct, err = verifier.VerifyConnectToken(getHMACTokenWithClaims(map[string]any{"sub": "42", "auth_class": 1}), false)
	require.NoError(t, err)
	require.Equal(t, "", ct.AuthClass)
}
