package jwtverify

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/centrifugal/centrifuge"
	"github.com/cristalhq/jwt/v5"
	"github.com/stretchr/testify/require"
)

// subscriptionClaimsToken returns an HS256 connection token for user with the given channels and subs claims. The
// namespace of the channels is not declared in the default config.
func subscriptionClaimsToken(t *testing.T, exp time.Time, channels []string, subs map[string]SubscribeOptions) string {
	t.Helper()
	claims := &ConnectTokenClaims{
		Channels: channels,
		Subs:     subs,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user",
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	}
	token, err := getECDSATokenBuilder(nil).Build(claims)
	require.NoError(t, err)
	return token.String()
}

func subscriptionClaimsVerifier(t *testing.T, ignore bool) *VerifierJWT {
	t.Helper()
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	verifier, err := NewTokenVerifierJWT(VerifierConfig{HMACSecretKey: "secret", IgnoreSubscriptionClaims: ignore}, cfgContainer)
	require.NoError(t, err)
	return verifier
}

func TestConnectTokenChannelOfUnknownNamespaceIsRefusedByDefault(t *testing.T) {
	verifier := subscriptionClaimsVerifier(t, false)
	future := time.Now().Add(time.Hour)

	_, err := verifier.VerifyConnectToken(subscriptionClaimsToken(t, future, []string{"updates:session"}, nil), false)
	require.ErrorIs(t, err, centrifuge.ErrorUnknownChannel)

	subs := map[string]SubscribeOptions{"updates:session": {}}
	_, err = verifier.VerifyConnectToken(subscriptionClaimsToken(t, future, nil, subs), false)
	require.ErrorIs(t, err, centrifuge.ErrorUnknownChannel)
}

func TestConnectTokenSubscriptionClaimsAreIgnoredWhenConfigured(t *testing.T) {
	verifier := subscriptionClaimsVerifier(t, true)
	future := time.Now().Add(time.Hour)

	ct, err := verifier.VerifyConnectToken(subscriptionClaimsToken(t, future, []string{"updates:session"}, nil), false)
	require.NoError(t, err)
	require.Equal(t, "user", ct.UserID)
	require.Empty(t, ct.Subs)

	subs := map[string]SubscribeOptions{"updates:session": {}}
	ct, err = verifier.VerifyConnectToken(subscriptionClaimsToken(t, future, nil, subs), false)
	require.NoError(t, err)
	require.Equal(t, "user", ct.UserID)
	require.Empty(t, ct.Subs)
}

func TestConnectTokenWithIgnoredSubscriptionClaimsIsStillVerified(t *testing.T) {
	verifier := subscriptionClaimsVerifier(t, true)

	_, err := verifier.VerifyConnectToken(subscriptionClaimsToken(t, time.Now().Add(-time.Minute), []string{"updates:session"}, nil), false)
	require.ErrorIs(t, err, ErrTokenExpired)

	other, err := NewTokenVerifierJWT(VerifierConfig{HMACSecretKey: "another secret", IgnoreSubscriptionClaims: true}, verifier.cfgContainer)
	require.NoError(t, err)
	_, err = other.VerifyConnectToken(subscriptionClaimsToken(t, time.Now().Add(time.Hour), []string{"updates:session"}, nil), false)
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestReloadAppliesIgnoreSubscriptionClaims(t *testing.T) {
	verifier := subscriptionClaimsVerifier(t, false)
	token := subscriptionClaimsToken(t, time.Now().Add(time.Hour), []string{"updates:session"}, nil)

	apply, discard, err := verifier.PrepareReload(VerifierConfig{HMACSecretKey: "secret", IgnoreSubscriptionClaims: true})
	require.NoError(t, err)
	defer discard()
	apply()

	ct, err := verifier.VerifyConnectToken(token, false)
	require.NoError(t, err)
	require.Empty(t, ct.Subs)
}
