package api

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// TestAccessRevokeAPIRefusesExpireAtBeyondMaxTTL refuses an expire_at beyond now plus max_ttl, and accepts one within it.
func TestAccessRevokeAPIRefusesExpireAtBeyondMaxTTL(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.AccessRevoke.MaxTTL = configtypes.Duration(time.Hour)
	api, store, events := newAccessRevokeExecutor(t, cfg, 100)
	ctx := context.Background()
	beyond := time.Now().Add(2 * time.Hour).Unix()

	blockResp := api.BlockUser(ctx, &BlockUserRequest{User: "user", ExpireAt: beyond})
	require.Equal(t, ErrorBadRequest, blockResp.Error)
	require.Equal(t, accessrevoke.Allowed, userVerdict(store, "user"))

	revokeResp := api.RevokeToken(ctx, &RevokeTokenRequest{User: "user", Uid: "session", ExpireAt: beyond})
	require.Equal(t, ErrorBadRequest, revokeResp.Error)
	require.Empty(t, *events)

	within := time.Now().Add(30 * time.Minute).Unix()
	okResp := api.BlockUser(ctx, &BlockUserRequest{User: "user", ExpireAt: within})
	require.Nil(t, okResp.Error)
	require.Equal(t, accessrevoke.Blocked, userVerdict(store, "user"))
}
