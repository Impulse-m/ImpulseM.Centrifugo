package push

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

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
