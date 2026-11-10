package push

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// TestDryRunLoadsNoWebPushKey checks that a dry run with Web Push enabled starts with a VAPID key file that does not exist:
// a dry run loads no key and opens no connection, so the missing file is no error, and a valid Web Push target is answered
// dry_run.
func TestDryRunLoadsNoWebPushKey(t *testing.T) {
	cfg := testConfig()
	cfg.DryRun = true
	cfg.WebPush.VAPIDPrivateKeyFile = filepath.Join(t.TempDir(), "missing.key")

	s, err := Setup(context.Background(), cfg)
	require.NoError(t, err)

	resp := s.Send(context.Background(), webPushOnly())

	require.Nil(t, resp.GetError())
	results := resp.GetResult().GetResults()
	require.Len(t, results, 1)
	require.Equal(t, provider.StatusDryRun, results[0].GetStatus())
}

// TestDryRunAnswersPayloadTooLarge checks that an oversized payload of each provider is answered payload_too_large in a
// dry run, as a live send answers it, and that no provider call is made in either mode.
func TestDryRunAnswersPayloadTooLarge(t *testing.T) {
	for _, dryRun := range []bool{true, false} {
		fake := &countingSender{}
		s := newTestSender(t, fake, nil)
		if dryRun {
			s.set = provider.NewDryRunSet().WithOversizeCheck(oversized)
		} else {
			s.set = provider.NewSet().WithOversizeCheck(oversized)
			s.set.Add(provider.ProviderAPNs, fake, 500)
			s.set.Add(provider.ProviderFCM, fake, 200)
		}

		req := validRequest()
		req.Notification.Apns.Payload = []byte(`{"d":"` + strings.Repeat("a", 5000) + `"}`)
		req.Notification.Fcm.Message = []byte(`{"data":{"d":"` + strings.Repeat("b", 4100) + `"}}`)

		resp := s.Send(context.Background(), req)

		require.Nil(t, resp.GetError())
		results := resp.GetResult().GetResults()
		require.Len(t, results, 2)
		for _, result := range results {
			require.Equal(t, provider.StatusRejected, result.GetStatus(), "dry run %v", dryRun)
			require.Equal(t, provider.CodePayloadTooLarge, result.GetCode(), "dry run %v", dryRun)
		}
		require.Zero(t, fake.calls.Load(), "dry run %v", dryRun)
	}
}
