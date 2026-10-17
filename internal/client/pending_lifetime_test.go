package client

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

func TestPendingLifetimeExceeded(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	const maxLifetime = 5 * time.Minute
	tests := []struct {
		name        string
		class       rpcregistry.AuthClass
		token       jwtverify.ConnectToken
		maxLifetime time.Duration
		want        bool
	}{
		{
			name:        "pending over the limit by iat",
			class:       rpcregistry.AuthClassPending2FA,
			token:       jwtverify.ConnectToken{ExpireAt: now.Unix() + 600, IssuedAt: now.Unix()},
			maxLifetime: maxLifetime,
			want:        true,
		},
		{
			name:        "pending within the limit by iat",
			class:       rpcregistry.AuthClassPending2FA,
			token:       jwtverify.ConnectToken{ExpireAt: now.Unix() + 240, IssuedAt: now.Unix()},
			maxLifetime: maxLifetime,
			want:        false,
		},
		{
			name:        "pending at the limit is allowed",
			class:       rpcregistry.AuthClassPending2FA,
			token:       jwtverify.ConnectToken{ExpireAt: now.Unix() + 300, IssuedAt: now.Unix()},
			maxLifetime: maxLifetime,
			want:        false,
		},
		{
			name:        "pending with an iat in the future is over the limit by now",
			class:       rpcregistry.AuthClassPending2FA,
			token:       jwtverify.ConnectToken{ExpireAt: now.Unix() + 3600 + 240, IssuedAt: now.Unix() + 3600},
			maxLifetime: maxLifetime,
			want:        true,
		},
		{
			name:        "pending with an iat in the future within the limit by now",
			class:       rpcregistry.AuthClassPending2FA,
			token:       jwtverify.ConnectToken{ExpireAt: now.Unix() + 240, IssuedAt: now.Unix() + 3600},
			maxLifetime: maxLifetime,
			want:        false,
		},
		{
			name:        "pending with an iat after exp is over the limit by now",
			class:       rpcregistry.AuthClassPending2FA,
			token:       jwtverify.ConnectToken{ExpireAt: now.Unix() + 600, IssuedAt: now.Unix() + 3600},
			maxLifetime: maxLifetime,
			want:        true,
		},
		{
			name:        "pending over the limit by now without iat",
			class:       rpcregistry.AuthClassPending2FA,
			token:       jwtverify.ConnectToken{ExpireAt: now.Unix() + 301},
			maxLifetime: maxLifetime,
			want:        true,
		},
		{
			name:        "pending within the limit by exp without iat is over the limit",
			class:       rpcregistry.AuthClassPending2FA,
			token:       jwtverify.ConnectToken{ExpireAt: now.Unix() + 120},
			maxLifetime: maxLifetime,
			want:        true,
		},
		{
			name:        "pending with a negative exp is over the limit like one without exp",
			class:       rpcregistry.AuthClassPending2FA,
			token:       jwtverify.ConnectToken{ExpireAt: -1, IssuedAt: now.Unix()},
			maxLifetime: maxLifetime,
			want:        true,
		},
		{
			name:        "pending with an iat of zero is over the limit like one without iat",
			class:       rpcregistry.AuthClassPending2FA,
			token:       jwtverify.ConnectToken{ExpireAt: now.Unix() + 120, IssuedAt: 0},
			maxLifetime: maxLifetime,
			want:        true,
		},
		{
			name:        "pending with a negative iat is over the limit like one without iat",
			class:       rpcregistry.AuthClassPending2FA,
			token:       jwtverify.ConnectToken{ExpireAt: now.Unix() + 120, IssuedAt: -1},
			maxLifetime: maxLifetime,
			want:        true,
		},
		{
			name:        "pending with a negative iat does not overflow the lifetime",
			class:       rpcregistry.AuthClassPending2FA,
			token:       jwtverify.ConnectToken{ExpireAt: 4611686018427387904, IssuedAt: -4611686018427387904},
			maxLifetime: maxLifetime,
			want:        true,
		},
		{
			name:        "pending issued long before the check without iat is over the limit",
			class:       rpcregistry.AuthClassPending2FA,
			token:       jwtverify.ConnectToken{ExpireAt: now.Unix() + 240},
			maxLifetime: maxLifetime,
			want:        true,
		},
		{
			name:        "pending without exp has no end",
			class:       rpcregistry.AuthClassPending2FA,
			token:       jwtverify.ConnectToken{IssuedAt: now.Unix()},
			maxLifetime: maxLifetime,
			want:        true,
		},
		{
			name:        "pending with an expiration beyond any duration",
			class:       rpcregistry.AuthClassPending2FA,
			token:       jwtverify.ConnectToken{ExpireAt: math.MaxInt64, IssuedAt: now.Unix()},
			maxLifetime: maxLifetime,
			want:        true,
		},
		{
			name:        "pending without a limit",
			class:       rpcregistry.AuthClassPending2FA,
			token:       jwtverify.ConnectToken{ExpireAt: now.Unix() + 86400, IssuedAt: now.Unix()},
			maxLifetime: 0,
			want:        false,
		},
		{
			name:        "authenticated over the limit is not limited",
			class:       rpcregistry.AuthClassAuthenticated,
			token:       jwtverify.ConnectToken{ExpireAt: now.Unix() + 86400, IssuedAt: now.Unix()},
			maxLifetime: maxLifetime,
			want:        false,
		},
		{
			name:        "anonymous without exp is not limited",
			class:       rpcregistry.AuthClassAnonymous,
			token:       jwtverify.ConnectToken{},
			maxLifetime: maxLifetime,
			want:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, pendingLifetimeExceeded(tt.class, tt.token, tt.maxLifetime, fixedClock(now)))
		})
	}
}

func TestPendingLifetimeClockIsNotReadWithoutLimit(t *testing.T) {
	clock := func() time.Time {
		t.Fatal("the clock is read without a limit")
		return time.Time{}
	}
	token := jwtverify.ConnectToken{ExpireAt: 1, IssuedAt: 1}
	require.False(t, pendingLifetimeExceeded(rpcregistry.AuthClassPending2FA, token, 0, clock))
	require.False(t, pendingLifetimeExceeded(rpcregistry.AuthClassAuthenticated, token, time.Minute, clock))
}

// fixedClock returns a clock which reads now.
func fixedClock(now time.Time) func() time.Time {
	return func() time.Time {
		return now
	}
}

// pendingLifetimeToken returns a connection token of class pending_2fa with user 42, which lives for lifetime from its
// issue time. Without iat the claim is left out, and such a token is over any lifetime limit.
func pendingLifetimeToken(t *testing.T, lifetime time.Duration, withIat bool) string {
	t.Helper()
	now := time.Now().Unix()
	claims := map[string]any{
		"sub":        "42",
		"jti":        revokeTestUUID,
		"exp":        now + int64(lifetime/time.Second),
		"auth_class": "pending_2fa",
	}
	if withIat {
		claims["iat"] = now
	}
	return getConnTokenWithClaims(t, claims)
}

func TestPendingConnectOverLifetimeLimitIsRefused(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.RPC.Registry.PendingMaxLifetime = configtypes.Duration(5 * time.Minute)
	h, _, _ := newAuthClassTestHandler(t, cfg)

	_, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c1",
		Token:    pendingLifetimeToken(t, 10*time.Minute, true),
	}, nil, false)
	require.Equal(t, centrifuge.DisconnectInvalidToken, err)

	_, err = h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c1",
		Token:    pendingLifetimeToken(t, 10*time.Minute, false),
	}, nil, false)
	require.Equal(t, centrifuge.DisconnectInvalidToken, err)

	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c1",
		Token:    pendingLifetimeToken(t, 4*time.Minute, true),
	}, nil, false)
	require.NoError(t, err)
	require.Empty(t, reply.Credentials.UserID)
}

func TestPendingConnectWithoutLifetimeLimitIsAllowed(t *testing.T) {
	h, _, _ := newAuthClassTestHandler(t, config.DefaultConfig())

	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c1",
		Token:    pendingLifetimeToken(t, 24*time.Hour, true),
	}, nil, false)
	require.NoError(t, err)
	require.Empty(t, reply.Credentials.UserID)
}

func TestPendingRefreshOverLifetimeLimitIsRefused(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.RPC.Registry.PendingMaxLifetime = configtypes.Duration(5 * time.Minute)
	h, _, node := newAuthClassTestHandler(t, cfg)

	client := connectRevokeTestClient(t, node, pendingLifetimeToken(t, 4*time.Minute, true))
	require.Empty(t, client.UserID())

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: pendingLifetimeToken(t, 10*time.Minute, true),
	}, nil)
	require.Equal(t, centrifuge.DisconnectInvalidToken, err)

	_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: pendingLifetimeToken(t, 4*time.Minute, true),
	}, nil)
	require.NoError(t, err)
	require.Empty(t, client.UserID())
}
