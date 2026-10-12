package api

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

var allAccessChecks = accessrevoke.CheckOptions{Block: true, Session: true, UserTokens: true}

// newAccessRevokeExecutor creates an Executor with access revocation wired in. Returned events slice
// collects cluster notifications sent by the Executor.
func newAccessRevokeExecutor(t *testing.T, cfg config.Config, notifyRate int) (*Executor, *accessrevoke.Store, *[]centrifuge.NotificationEvent) {
	t.Helper()
	node := nodeWithMemoryEngine()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	events := &[]centrifuge.NotificationEvent{}
	node.OnNotification(func(event centrifuge.NotificationEvent) {
		*events = append(*events, event)
	})

	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)

	store := accessrevoke.NewStore(time.Minute)
	revoker := accessrevoke.NewRevoker(store, node.Hub(), accessrevoke.NewDisconnectQueue(1000, 100))
	executor := NewExecutor(node, cfgContainer, &testSurveyCaller{}, ExecutorConfig{
		Protocol:       "test",
		AccessRevoker:  revoker,
		AccessNotifier: accessrevoke.NewNotifier(node, notifyRate),
	})
	return executor, store, events
}

func userVerdict(store *accessrevoke.Store, userID string) accessrevoke.Verdict {
	return store.Check(userID, centrifuge.TokenIdentity{}, accessrevoke.CheckOptions{Block: true}).Verdict
}

func sessionVerdict(store *accessrevoke.Store, sessionID string, issuedAt int64) accessrevoke.Verdict {
	return store.Check("", accessrevoke.Identity(sessionID, 0, issuedAt), allAccessChecks).Verdict
}

func TestBlockUserAPI(t *testing.T) {
	api, store, events := newAccessRevokeExecutor(t, config.DefaultConfig(), 100)

	resp := api.BlockUser(context.Background(), &BlockUserRequest{})
	require.Equal(t, ErrorBadRequest, resp.Error)
	require.Equal(t, accessrevoke.Allowed, userVerdict(store, ""))

	resp = api.BlockUser(context.Background(), &BlockUserRequest{User: "user"})
	require.Nil(t, resp.Error)
	require.NotNil(t, resp.Result)
	require.Equal(t, accessrevoke.Blocked, userVerdict(store, "user"))

	// Applied change is sent to other nodes with resolved values.
	require.Len(t, *events, 1)
	require.Equal(t, accessrevoke.NotifyOp, (*events)[0].Op)
	require.Equal(t, api.node.ID(), (*events)[0].FromNodeID)
	notification, err := accessrevoke.DecodeNotification((*events)[0].Data)
	require.NoError(t, err)
	require.Equal(t, accessrevoke.KindBlockUser, notification.Kind)
	require.Equal(t, "user", notification.User)
	require.NotZero(t, notification.ExpireAt)
}

func TestBlockUserAPIDisabled(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.UserBlock.Enabled = false
	api, store, events := newAccessRevokeExecutor(t, cfg, 100)

	resp := api.BlockUser(context.Background(), &BlockUserRequest{User: "user"})
	require.Equal(t, ErrorNotAvailable, resp.Error)
	require.Equal(t, accessrevoke.Allowed, userVerdict(store, "user"))
	require.Empty(t, *events)
}

func TestUnblockUserAPI(t *testing.T) {
	api, store, events := newAccessRevokeExecutor(t, config.DefaultConfig(), 100)

	resp := api.UnblockUser(context.Background(), &UnblockUserRequest{})
	require.Equal(t, ErrorBadRequest, resp.Error)

	blockResp := api.BlockUser(context.Background(), &BlockUserRequest{User: "user"})
	require.Nil(t, blockResp.Error)
	require.Equal(t, accessrevoke.Blocked, userVerdict(store, "user"))

	resp = api.UnblockUser(context.Background(), &UnblockUserRequest{User: "user"})
	require.Nil(t, resp.Error)
	require.NotNil(t, resp.Result)
	require.Equal(t, accessrevoke.Allowed, userVerdict(store, "user"))
	require.Len(t, *events, 2)
}

func TestUnblockUserAPIDisabled(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.UserBlock.Enabled = false
	api, _, _ := newAccessRevokeExecutor(t, cfg, 100)

	resp := api.UnblockUser(context.Background(), &UnblockUserRequest{User: "user"})
	require.Equal(t, ErrorNotAvailable, resp.Error)
}

func TestRevokeTokenAPI(t *testing.T) {
	api, store, events := newAccessRevokeExecutor(t, config.DefaultConfig(), 100)

	resp := api.RevokeToken(context.Background(), &RevokeTokenRequest{User: "user"})
	require.Equal(t, ErrorBadRequest, resp.Error)
	require.Equal(t, accessrevoke.Allowed, sessionVerdict(store, "session", 500))

	resp = api.RevokeToken(context.Background(), &RevokeTokenRequest{User: "user", Uid: "session", RevokedAt: 1000})
	require.Nil(t, resp.Error)
	require.NotNil(t, resp.Result)
	require.Equal(t, accessrevoke.Revoked, sessionVerdict(store, "session", 500))
	require.Equal(t, accessrevoke.Allowed, sessionVerdict(store, "other", 500))

	require.Len(t, *events, 1)
	notification, err := accessrevoke.DecodeNotification((*events)[0].Data)
	require.NoError(t, err)
	require.Equal(t, accessrevoke.KindRevokeToken, notification.Kind)
	require.Equal(t, "session", notification.Session)
	require.Equal(t, int64(1000), notification.RevokedAt)
}

func TestRevokeTokenAPIDisabled(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.TokenRevoke.Enabled = false
	api, store, _ := newAccessRevokeExecutor(t, cfg, 100)

	resp := api.RevokeToken(context.Background(), &RevokeTokenRequest{Uid: "session", RevokedAt: 1000})
	require.Equal(t, ErrorNotAvailable, resp.Error)
	require.Equal(t, accessrevoke.Allowed, sessionVerdict(store, "session", 500))
}

func TestInvalidateUserTokensAPI(t *testing.T) {
	api, store, _ := newAccessRevokeExecutor(t, config.DefaultConfig(), 100)
	ctx := context.Background()

	resp := api.InvalidateUserTokens(ctx, &InvalidateUserTokensRequest{Generation: 5})
	require.Equal(t, ErrorBadRequest, resp.Error)

	resp = api.InvalidateUserTokens(ctx, &InvalidateUserTokensRequest{User: "user", Channel: "chat:index", Generation: 5})
	require.Equal(t, ErrorNotAvailable, resp.Error)

	resp = api.InvalidateUserTokens(ctx, &InvalidateUserTokensRequest{User: "user", Generation: 5, RevokedAt: 1000})
	require.Nil(t, resp.Error)
	require.NotNil(t, resp.Result)
	require.Equal(t, accessrevoke.Revoked, store.Check("user", accessrevoke.Identity("s", 4, 500), allAccessChecks).Verdict)
	require.Equal(t, accessrevoke.Allowed, store.Check("user", accessrevoke.Identity("s", 5, 500), allAccessChecks).Verdict)
}

func TestInvalidateUserTokensAPIDisabled(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.UserTokensInvalidate.Enabled = false
	api, _, _ := newAccessRevokeExecutor(t, cfg, 100)

	resp := api.InvalidateUserTokens(context.Background(), &InvalidateUserTokensRequest{User: "user", Generation: 5})
	require.Equal(t, ErrorNotAvailable, resp.Error)
}

func TestInvalidateUserTokensAPIIssuedBeforeDefault(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Unix()

	// Neither generation nor issued_before: issued_before defaults to now.
	api, store, _ := newAccessRevokeExecutor(t, config.DefaultConfig(), 100)
	resp := api.InvalidateUserTokens(ctx, &InvalidateUserTokensRequest{User: "user"})
	require.Nil(t, resp.Error)
	require.Equal(t, accessrevoke.Revoked, store.Check("user", accessrevoke.Identity("s", 0, now-100), allAccessChecks).Verdict)

	// Generation is set: issued_before is not defaulted, so an old token of the new generation is allowed.
	api, store, _ = newAccessRevokeExecutor(t, config.DefaultConfig(), 100)
	resp = api.InvalidateUserTokens(ctx, &InvalidateUserTokensRequest{User: "user", Generation: 5})
	require.Nil(t, resp.Error)
	require.Equal(t, accessrevoke.Allowed, store.Check("user", accessrevoke.Identity("s", 5, 1), allAccessChecks).Verdict)

	// Generation is set and issued_before is given: it is used as is.
	api, store, _ = newAccessRevokeExecutor(t, config.DefaultConfig(), 100)
	resp = api.InvalidateUserTokens(ctx, &InvalidateUserTokensRequest{User: "user", Generation: 5, IssuedBefore: 300, RevokedAt: now})
	require.Nil(t, resp.Error)
	require.Equal(t, accessrevoke.Revoked, store.Check("user", accessrevoke.Identity("s", 5, 200), allAccessChecks).Verdict)
	require.Equal(t, accessrevoke.Allowed, store.Check("user", accessrevoke.Identity("s", 5, 400), allAccessChecks).Verdict)
}

func TestAccessRevokeAPIRateLimit(t *testing.T) {
	api, store, events := newAccessRevokeExecutor(t, config.DefaultConfig(), 1)
	ctx := context.Background()

	resp := api.BlockUser(ctx, &BlockUserRequest{User: "first"})
	require.Nil(t, resp.Error)

	// Rate limit is exceeded: nothing is applied and nothing is sent.
	resp = api.BlockUser(ctx, &BlockUserRequest{User: "second"})
	require.Equal(t, ErrorTooManyRequests, resp.Error)
	require.Equal(t, accessrevoke.Allowed, userVerdict(store, "second"))
	require.Len(t, *events, 1)

	revokeResp := api.RevokeToken(ctx, &RevokeTokenRequest{Uid: "session", RevokedAt: 1000})
	require.Equal(t, ErrorTooManyRequests, revokeResp.Error)
	require.Equal(t, accessrevoke.Allowed, sessionVerdict(store, "session", 500))
}
