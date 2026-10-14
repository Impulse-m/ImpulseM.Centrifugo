package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// newRegistryRebindHandler returns a handler with the registry of the auth class tests and rebind enabled.
func newRegistryRebindHandler(t *testing.T) *Handler {
	t.Helper()
	backend := newRegistryTestBackend(t)
	reg := registryAuthClassTestRegistry()
	registry := startRegistryTestRegistry(t, reg, backend)
	cfg := config.DefaultConfig()
	cfg.RPC.Registry = reg
	cfg.Client.Rebind = configtypes.Rebind{Enabled: true}
	h, _ := newRegistryTestHandlerWithClaim(t, cfg, registry, "auth_class")
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)
	return h
}

func TestConnectReplyCarriesUserAndAuthClass(t *testing.T) {
	h, _ := newRegistryAuthClassHandler(t)

	pending, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42", "auth_class": "pending_2fa"}),
	}, nil, false)
	require.NoError(t, err)
	require.Equal(t, "42", pending.User)
	require.Equal(t, "pending_2fa", pending.AuthClass)
	require.Empty(t, pending.Credentials.UserID)

	authenticated, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42", "auth_class": "authenticated"}),
	}, nil, false)
	require.NoError(t, err)
	require.Equal(t, "42", authenticated.User)
	require.Equal(t, "authenticated", authenticated.AuthClass)
	require.Equal(t, "42", authenticated.Credentials.UserID)
}

func TestConnectReplyWithoutRegistryHasUserOnly(t *testing.T) {
	h, _ := newRebindTestHandler(t, configtypes.Rebind{}, false)

	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42"}),
	}, nil, false)
	require.NoError(t, err)
	require.Equal(t, "42", reply.User)
	require.Empty(t, reply.AuthClass)
}

func TestRefreshReplyCarriesUserAndAuthClass(t *testing.T) {
	h, _ := newRegistryAuthClassHandler(t)
	client, _ := newRebindTestClient("42", "authenticated")

	reply, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42", "auth_class": "authenticated"}),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "42", reply.User)
	require.Equal(t, "authenticated", reply.AuthClass)
}

func TestRebindRefreshReplyCarriesUserAndAuthClass(t *testing.T) {
	h := newRegistryRebindHandler(t)
	client, state := newRebindTestClient("", "anonymous")

	reply, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42", "auth_class": "authenticated", "jti": revokeTestOtherUUID}),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "42", state.user)
	require.Equal(t, "42", reply.User)
	require.Equal(t, "authenticated", reply.AuthClass)
}
