package client

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// TestRebindChannelLimitIsReadAtSetup checks that the upgrade precheck uses the client.channel_limit the node runs with,
// as Core does. A reload which raises the limit does not admit an upgrade which Core would refuse after the identity has
// moved: the upgrade is refused before the identity moves, and the connection keeps its identity and its channels.
func TestRebindChannelLimitIsReadAtSetup(t *testing.T) {
	now := time.Now().Unix()
	cfg := newRebindTestConfig(configtypes.Rebind{Enabled: true}, configtypes.RateLimitUserCommand{})
	cfg.Client.ChannelLimit = 2
	h, _ := newRegistryTestHandlerWithClaim(t, cfg, nil, rebindTestClaim)
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)
	require.NoError(t, h.Setup())

	reloaded := cfg
	reloaded.Client.ChannelLimit = 256
	require.NoError(t, h.cfgContainer.Reload(reloaded))

	// Two client channels fill the limit at start, so the personal channel of the upgrade does not fit.
	refused, refusedState := newRebindTestClient("", "anonymous")
	refusedState.channels["chat:one"] = centrifuge.ChannelContext{}
	refusedState.channels["chat:two"] = centrifuge.ChannelContext{}
	before := refused.ExtState().(*connState).identity.Load()
	_, _, err := h.OnRefresh(refused, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "45", "authenticated", revokeTestUUID, map[string]any{"iat": now}),
	}, nil)
	require.Equal(t, centrifuge.ErrorLimitExceeded, err)
	require.Empty(t, refusedState.rebinds)
	require.Empty(t, refused.UserID())
	require.Same(t, before, refused.ExtState().(*connState).identity.Load())
	require.Len(t, refusedState.channels, 2)
}
