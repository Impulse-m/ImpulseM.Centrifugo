package api

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func clampCount(t *testing.T, source string) float64 {
	t.Helper()
	return testutil.ToFloat64(metrics.AccessRevokeExpireAtClampedTotal.WithLabelValues(source))
}

// TestConsumingAccessRevokeClampsBeyondMaxTTL applies a block_user from a consumer with an expire_at far beyond the max
// TTL. The block is applied, and its expire_at is clamped to now plus the max TTL, counted under the consumer source.
func TestConsumingAccessRevokeClampsBeyondMaxTTL(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.AccessRevoke.MaxTTL = configtypes.Duration(time.Hour)
	api, store, events := newAccessRevokeExecutor(t, cfg, 100)
	api.config.AccessRevoker.SetMaxTTL(func() time.Duration {
		return time.Hour
	})
	dispatcher := NewDispatcher(NewConsumingHandler(api.node, api, ConsumingHandlerConfig{}))
	ctx := context.Background()
	clampsBefore := clampCount(t, accessrevoke.ClampSourceConsumer)

	farExpireAt := time.Now().Add(30 * 24 * time.Hour).Unix()
	before := time.Now().Unix()
	err := dispatcher.DispatchCommand(ctx, "block_user", []byte(`{"user":"user","expire_at":`+strconv.FormatInt(farExpireAt, 10)+`}`))
	require.NoError(t, err)
	after := time.Now().Unix()

	require.Equal(t, accessrevoke.Blocked, userVerdict(store, "user"))
	require.Len(t, *events, 1)
	notification, err := accessrevoke.DecodeNotification((*events)[0].Data)
	require.NoError(t, err)
	require.GreaterOrEqual(t, notification.ExpireAt, before+int64(time.Hour/time.Second))
	require.LessOrEqual(t, notification.ExpireAt, after+int64(time.Hour/time.Second))
	require.Equal(t, clampsBefore+1, clampCount(t, accessrevoke.ClampSourceConsumer))
}

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
