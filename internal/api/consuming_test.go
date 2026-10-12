package api

import (
	"context"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/stretchr/testify/require"
)

func TestNewConsumingHandler(t *testing.T) {
	n := nodeWithMemoryEngine()
	defer func() { _ = n.Shutdown(context.Background()) }()

	cfg := config.DefaultConfig()
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)

	handler := NewConsumingHandler(n, NewExecutor(n, cfgContainer, nil, ExecutorConfig{
		Protocol:         "consumer",
		UseOpenTelemetry: false,
	}), ConsumingHandlerConfig{})

	dispatcher := NewDispatcher(handler)

	// Bad request must be just logged but no errors other than Internal Error should be returned from Dispatch.
	err = dispatcher.DispatchCommand(context.Background(), "publish", []byte(`{}`))
	require.NoError(t, err)
}

func TestConsumingAccessRevoke(t *testing.T) {
	api, store, events := newAccessRevokeExecutor(t, config.DefaultConfig(), 100)
	dispatcher := NewDispatcher(NewConsumingHandler(api.node, api, ConsumingHandlerConfig{}))
	ctx := context.Background()

	err := dispatcher.DispatchCommand(ctx, "revoke_token", []byte(`{"user":"user","uid":"session","revoked_at":1000}`))
	require.NoError(t, err)
	require.Equal(t, accessrevoke.Revoked, sessionVerdict(store, "session", 500))
	require.Len(t, *events, 1)

	err = dispatcher.DispatchCommand(ctx, "block_user", []byte(`{"user":"user"}`))
	require.NoError(t, err)
	require.Equal(t, accessrevoke.Blocked, userVerdict(store, "user"))
	require.Len(t, *events, 2)

	// Invalid request is logged and skipped, not returned as error.
	err = dispatcher.DispatchCommand(ctx, "revoke_token", []byte(`{}`))
	require.NoError(t, err)
	require.Len(t, *events, 2)
}

func TestConsumingAccessRevokeRateLimitRetries(t *testing.T) {
	api, store, events := newAccessRevokeExecutor(t, config.DefaultConfig(), 1)
	dispatcher := NewDispatcher(NewConsumingHandler(api.node, api, ConsumingHandlerConfig{}))
	ctx := context.Background()

	err := dispatcher.DispatchCommand(ctx, "block_user", []byte(`{"user":"first"}`))
	require.NoError(t, err)

	// Rate limited revocation must be returned as error so that the consumer retries it.
	err = dispatcher.DispatchCommand(ctx, "revoke_token", []byte(`{"user":"user","uid":"session","revoked_at":1000}`))
	require.Error(t, err)
	require.Equal(t, accessrevoke.Allowed, sessionVerdict(store, "session", 500))
	require.Len(t, *events, 1)
}
