package client

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// chainTestUUID is the session of the last refresh of the chain tests.
const chainTestUUID = "7c1f0e2d-3b4a-4c5d-8e6f-9a0b1c2d3e4f"

// TestRebindChainKeepsAccount moves a connection to an anonymous token and then to a token of a user. The account of the
// connection survives the anonymous token, so the second step is denied unless its user is the account.
func TestRebindChainKeepsAccount(t *testing.T) {
	tests := []struct {
		name           string
		fromUser       string
		fromTokenUser  string
		fromClass      string
		allowDowngrade bool
		toClass        string
		toUser         string
		wantErr        error
	}{
		{
			name:          "pending to anonymous to pending of another user",
			fromTokenUser: "42",
			fromClass:     "pending_2fa",
			toClass:       "pending_2fa",
			toUser:        "43",
			wantErr:       centrifuge.ErrorPermissionDenied,
		},
		{
			name:          "pending to anonymous to authenticated of another user",
			fromTokenUser: "42",
			fromClass:     "pending_2fa",
			toClass:       "authenticated",
			toUser:        "43",
			wantErr:       centrifuge.ErrorPermissionDenied,
		},
		{
			name:           "authenticated to anonymous to pending of another user",
			fromUser:       "42",
			fromTokenUser:  "42",
			fromClass:      "authenticated",
			allowDowngrade: true,
			toClass:        "pending_2fa",
			toUser:         "43",
			wantErr:        centrifuge.ErrorPermissionDenied,
		},
		{
			name:           "authenticated to anonymous to authenticated of another user",
			fromUser:       "42",
			fromTokenUser:  "42",
			fromClass:      "authenticated",
			allowDowngrade: true,
			toClass:        "authenticated",
			toUser:         "43",
			wantErr:        centrifuge.ErrorPermissionDenied,
		},
		{
			name:          "pending to anonymous to pending of the same user",
			fromTokenUser: "42",
			fromClass:     "pending_2fa",
			toClass:       "pending_2fa",
			toUser:        "42",
		},
		{
			name:           "authenticated to anonymous to authenticated of the same user",
			fromUser:       "42",
			fromTokenUser:  "42",
			fromClass:      "authenticated",
			allowDowngrade: true,
			toClass:        "authenticated",
			toUser:         "42",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true, AllowDowngrade: tt.allowDowngrade}, false)
			client, state := newRebindTestClientWithToken(tt.fromUser, tt.fromTokenUser, tt.fromClass)

			_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
				Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, nil),
			}, nil)
			require.NoError(t, err)
			require.Empty(t, state.user)
			rebinds := len(state.rebinds)

			_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{
				Token: rebindTestToken(t, tt.toUser, tt.toClass, chainTestUUID, nil),
			}, nil)
			if tt.wantErr != nil {
				require.Equal(t, tt.wantErr, err)
				require.Len(t, state.rebinds, rebinds)
				return
			}
			require.NoError(t, err)
			require.Len(t, state.rebinds, rebinds+1)
		})
	}
}
