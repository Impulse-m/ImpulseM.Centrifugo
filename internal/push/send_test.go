package push

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

func TestSetupOffReturnsNil(t *testing.T) {
	s, err := Setup(context.Background(), configtypes.PushNotifications{Enabled: false, Sender: true})

	require.NoError(t, err)
	require.Nil(t, s)
}

func TestSetupNonSenderLoadsNothing(t *testing.T) {
	// The key files do not exist: a node that is not a sender must not read them, so the setup does not fail.
	cfg := testConfig()
	cfg.Sender = false
	cfg.APNs.TokenKeyFile = filepath.Join(t.TempDir(), "missing.p8")
	cfg.FCM.CredentialsFile = filepath.Join(t.TempDir(), "missing.json")

	s, err := Setup(context.Background(), cfg)

	require.NoError(t, err)
	require.NotNil(t, s)
	require.False(t, s.IsSender())
	require.Nil(t, s.set)
}

func TestSetupDryRunLoadsNothing(t *testing.T) {
	cfg := testConfig()
	cfg.DryRun = true
	cfg.APNs.TokenKeyFile = filepath.Join(t.TempDir(), "missing.p8")

	s, err := Setup(context.Background(), cfg)

	require.NoError(t, err)
	require.True(t, s.IsSender())
	require.NotNil(t, s.set)
}

func TestSetupRejectsBadKeyAtStartup(t *testing.T) {
	cfg := testConfig()
	cfg.EnabledProviders = []string{provider.ProviderAPNs}
	cfg.APNs.TokenKeyFile = filepath.Join(t.TempDir(), "missing.p8")

	s, err := Setup(context.Background(), cfg)

	require.Error(t, err)
	require.Nil(t, s)
	require.Contains(t, err.Error(), "push_notifications.apns.token_key_file")
}

func TestSendNilAnswers108(t *testing.T) {
	var s *Sender

	resp := s.Send(context.Background(), validRequest())

	require.Nil(t, resp.GetResult())
	require.Equal(t, uint32(108), resp.GetError().GetCode())
	require.Equal(t, "push notifications are not enabled", resp.GetError().GetMessage())
}

func TestSendNonSenderAnswers108(t *testing.T) {
	fake := &countingSender{}
	s := newTestSender(t, fake, func(cfg *configtypes.PushNotifications) {
		cfg.Sender = false
	})

	resp := s.Send(context.Background(), validRequest())

	require.Nil(t, resp.GetResult())
	require.Equal(t, uint32(108), resp.GetError().GetCode())
	require.Equal(t, "direct send is available on sender nodes only", resp.GetError().GetMessage())
	require.Zero(t, fake.calls.Load())
}

func TestDryRunSendPathMakesNoRequest(t *testing.T) {
	// A dry run set holds no provider, so the send path answers dry_run without any call.
	s := newTestSender(t, &countingSender{}, nil)
	s.set = provider.NewDryRunSet()

	resp := s.Send(context.Background(), validRequest())

	require.Nil(t, resp.GetError())
	require.Len(t, resp.GetResult().GetResults(), 2)
	for _, result := range resp.GetResult().GetResults() {
		require.Equal(t, provider.StatusDryRun, result.GetStatus())
		require.Zero(t, result.GetHttpStatus())
	}
}

func TestResultsInRequestOrder(t *testing.T) {
	fake := &countingSender{}
	s := newTestSender(t, fake, nil)

	req := &apiproto.SendPushNotificationRequest{
		Recipient: &apiproto.PushRecipient{},
		Notification: &apiproto.PushNotification{
			Fcm: &apiproto.FcmPushNotification{Message: testMessage()},
		},
	}
	for index := 0; index < 20; index++ {
		req.Recipient.Targets = append(req.Recipient.Targets, &apiproto.PushTarget{
			Provider: provider.ProviderFCM,
			Token:    fmt.Sprintf("token-%d", index),
			Ref:      fmt.Sprintf("ref-%d", index),
		})
	}

	resp := s.Send(context.Background(), req)

	require.Nil(t, resp.GetError())
	results := resp.GetResult().GetResults()
	require.Len(t, results, 20)
	for index, result := range results {
		require.Equal(t, uint32(index), result.GetIndex())
		require.Equal(t, fmt.Sprintf("ref-%d", index), result.GetRef())
		require.Equal(t, provider.StatusSent, result.GetStatus())
	}
	require.Equal(t, int32(20), fake.calls.Load())
}

func TestUIDGeneratedWhenEmptyAndEchoedWhenSet(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	generated := s.Send(context.Background(), validRequest())
	parsed, err := uuid.Parse(generated.GetResult().GetUid())
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, parsed)

	req := validRequest()
	req.Uid = "caller-uid"
	echoed := s.Send(context.Background(), req)
	require.Equal(t, "caller-uid", echoed.GetResult().GetUid())
}

func TestCallLimitAnswers111(t *testing.T) {
	s := newTestSender(t, &countingSender{}, func(cfg *configtypes.PushNotifications) {
		cfg.MaxConcurrentRequests = 1
	})

	// The one call slot of the node is taken, so the next call is refused without a queue.
	require.True(t, s.calls.TryAcquire(1))
	defer s.calls.Release(1)

	resp := s.Send(context.Background(), validRequest())

	require.Nil(t, resp.GetResult())
	require.Equal(t, uint32(111), resp.GetError().GetCode())
	require.True(t, resp.GetError().GetTemporary())
}

func TestNotSentAfterSyncTimeout(t *testing.T) {
	// One slot per provider, and a call that holds its slot until its request timeout. The second and third targets wait
	// for the slot, and the sync deadline ends the wait before the slot is free: they are not_sent with no_slot and make
	// no call.
	fake := &countingSender{hold: true}
	s := newTestSender(t, fake, func(cfg *configtypes.PushNotifications) {
		cfg.APNs.MaxInflight = 1
		cfg.SyncTimeout = configtypes.Duration(150 * time.Millisecond)
		cfg.Providers.RequestTimeout = configtypes.Duration(200 * time.Millisecond)
	})
	s.set = provider.NewSet()
	s.set.Add(provider.ProviderAPNs, fake, 1)

	req := validRequest()
	req.Recipient.Targets = []*apiproto.PushTarget{
		{Provider: provider.ProviderAPNs, Token: "a", Environment: "production"},
		{Provider: provider.ProviderAPNs, Token: "b", Environment: "production"},
		{Provider: provider.ProviderAPNs, Token: "c", Environment: "production"},
	}

	resp := s.Send(context.Background(), req)

	require.Nil(t, resp.GetError())
	var notSent int
	var answered int
	for _, result := range resp.GetResult().GetResults() {
		if result.GetStatus() == provider.StatusNotSent {
			require.Equal(t, provider.CodeNoSlot, result.GetCode())
			notSent++
			continue
		}
		answered++
	}
	require.GreaterOrEqual(t, notSent, 1)
	require.Equal(t, int32(answered), fake.calls.Load(), "a not_sent target makes no call")
}

func TestExpiredItemRejectedWithoutCall(t *testing.T) {
	fake := &countingSender{}
	s := newTestSender(t, fake, nil)

	req := validRequest()
	req.Notification.ExpireAt = time.Now().Unix() - 1

	resp := s.Send(context.Background(), req)

	require.Nil(t, resp.GetError())
	for _, result := range resp.GetResult().GetResults() {
		require.Equal(t, provider.StatusRejected, result.GetStatus())
		require.Equal(t, provider.CodeExpired, result.GetCode())
	}
	require.Zero(t, fake.calls.Load())
}

func TestWorkerPoolSize(t *testing.T) {
	require.Equal(t, 100, poolSize(100, 500), "APNs: min(100, 500)")
	require.Equal(t, 200, poolSize(500, 200), "FCM: min(500, 200)")
}

func TestDefaultCallTimeoutIsSyncPlusRequestPlusMargin(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	// The default bound is sync_timeout (10 s) plus providers.request_timeout (10 s) plus 5 s.
	require.Equal(t, 25*time.Second, s.DefaultCallTimeout())
	require.Equal(t, 25*time.Second, s.CallTimeout(0))
}
