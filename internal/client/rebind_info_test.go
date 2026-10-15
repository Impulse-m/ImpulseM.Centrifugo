package client

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// rebindInfoChannel is a client channel with presence, which an anonymous connection may keep through a rebind.
const rebindInfoChannel = "chan"

// rebindInfoConfig returns a config with rebind and downgrades on, and rebindInfoChannel with presence.
func rebindInfoConfig() config.Config {
	cfg := config.DefaultConfig()
	cfg.Client.Rebind.Enabled = true
	cfg.Client.Rebind.AllowDowngrade = true
	cfg.Channel.WithoutNamespace.Presence = true
	cfg.Channel.WithoutNamespace.SubscribeForAnonymous = true
	return cfg
}

// subscribeRebindInfoChannel subscribes client to rebindInfoChannel as a client subscription does, with presence.
func subscribeRebindInfoChannel(t *testing.T, client *centrifuge.Client) {
	t.Helper()
	err := client.Subscribe(rebindInfoChannel, subscribeOptionsOf(centrifuge.SubscribeOptions{
		EmitPresence: true,
		Source:       subsource.ClientAllowed,
	}))
	require.NoError(t, err)
}

// presenceConnInfo returns the connection info which the presence entry of client on rebindInfoChannel holds.
func presenceConnInfo(t *testing.T, node *centrifuge.Node, client *centrifuge.Client) []byte {
	t.Helper()
	result, err := node.Presence(rebindInfoChannel)
	require.NoError(t, err)
	info, ok := result.Presence[client.ID()]
	require.True(t, ok, "no presence entry for the connection")
	return info.ConnInfo
}

func TestRebindUpgradeSetsConnInfo(t *testing.T) {
	h, _, node := newAuthClassTestHandler(t, rebindInfoConfig())
	client := connectRevokeTestClient(t, node, rebindTestToken(t, "", "anonymous", revokeTestUUID, nil))
	require.Empty(t, client.UserID())
	subscribeRebindInfoChannel(t, client)
	require.Empty(t, presenceConnInfo(t, node, client))

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, map[string]any{
			"info": map[string]any{"name": "x"},
		}),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "42", client.UserID())
	require.JSONEq(t, `{"name":"x"}`, string(presenceConnInfo(t, node, client)))
}

func TestRebindDowngradeClearsConnInfo(t *testing.T) {
	h, _, node := newAuthClassTestHandler(t, rebindInfoConfig())
	client := connectRevokeTestClient(t, node, rebindTestToken(t, "42", "authenticated", revokeTestUUID, map[string]any{
		"info": map[string]any{"name": "x"},
	}))
	require.Equal(t, "42", client.UserID())
	subscribeRebindInfoChannel(t, client)
	require.JSONEq(t, `{"name":"x"}`, string(presenceConnInfo(t, node, client)))

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.Empty(t, client.UserID())
	require.Empty(t, presenceConnInfo(t, node, client))
}
