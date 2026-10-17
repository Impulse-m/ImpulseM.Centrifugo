package client

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// TestRebindDowngradeFollowsRunningConcurrency checks that a logout of a connection with a user is refused while the
// handler runs with a concurrency above one, even when the config allows the downgrade. A reload which allows it does
// not change the running concurrency, which is read at start. With one command at a time the downgrade is allowed.
func TestRebindDowngradeFollowsRunningConcurrency(t *testing.T) {
	tests := []struct {
		name               string
		runningConcurrency int
		wantErr            error
	}{
		{name: "one command at a time allows the downgrade", runningConcurrency: 1},
		{name: "concurrency above one refuses the downgrade", runningConcurrency: 4, wantErr: centrifuge.DisconnectInvalidToken},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true, AllowDowngrade: true}, false)
			h.runningConcurrency = tt.runningConcurrency
			client, state := newRebindTestClientWithToken("42", "42", "authenticated")

			_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
				Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, nil),
			}, nil)
			if tt.wantErr != nil {
				require.Equal(t, tt.wantErr, err)
				require.Empty(t, state.rebinds)
				require.Equal(t, "42", state.user)
				return
			}
			require.NoError(t, err)
			require.Len(t, state.rebinds, 1)
			require.Empty(t, state.user)
		})
	}
}
