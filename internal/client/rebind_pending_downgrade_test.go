package client

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// TestRebindPendingDowngradeFollowsRunningConcurrency checks that a pending connection, which holds the user of its token
// while Core sees no user, is refused a downgrade to an anonymous token above one command at a time, as an authenticated
// connection is. With one command at a time the downgrade is allowed, as the transition table allows it.
func TestRebindPendingDowngradeFollowsRunningConcurrency(t *testing.T) {
	tests := []struct {
		name               string
		allowDowngrade     bool
		runningConcurrency int
		wantErr            error
	}{
		{name: "one command at a time allows the downgrade", allowDowngrade: false, runningConcurrency: 1},
		{name: "concurrency above one refuses the downgrade", allowDowngrade: false, runningConcurrency: 4, wantErr: centrifuge.DisconnectInvalidToken},
		{name: "concurrency above one refuses the downgrade with allow_downgrade", allowDowngrade: true, runningConcurrency: 4, wantErr: centrifuge.DisconnectInvalidToken},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true, AllowDowngrade: tt.allowDowngrade}, false)
			h.runningConcurrency = tt.runningConcurrency
			client, state := newRebindTestClientWithToken("", "42", "pending_2fa")

			_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
				Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, nil),
			}, nil)
			if tt.wantErr != nil {
				require.Equal(t, tt.wantErr, err)
				require.Empty(t, state.rebinds)
				require.Empty(t, state.user)
				require.Equal(t, "42", connStateOf(client).identity.Load().tokenUser)
				return
			}
			require.NoError(t, err)
			require.Len(t, state.rebinds, 1)
			require.Empty(t, state.user)
		})
	}
}
