package accessrevoke

import (
	"testing"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

func TestRejectDisconnect(t *testing.T) {
	revoked := centrifuge.DisconnectExpired
	tests := []struct {
		name       string
		verdict    Verdict
		want       centrifuge.Disconnect
		wantReject bool
	}{
		{name: "allowed", verdict: Allowed, want: centrifuge.Disconnect{}, wantReject: false},
		{name: "blocked", verdict: Blocked, want: DisconnectUserBlocked, wantReject: true},
		{name: "revoked", verdict: Revoked, want: revoked, wantReject: true},
		{name: "loop guard", verdict: LoopGuard, want: centrifuge.DisconnectInvalidToken, wantReject: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			disconnect, rejected := RejectDisconnect(Result{Verdict: tt.verdict}, revoked)
			require.Equal(t, tt.wantReject, rejected)
			require.Equal(t, tt.want, disconnect)
		})
	}
}
