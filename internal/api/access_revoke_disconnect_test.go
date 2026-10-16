package api

import (
	"context"
	"strings"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/stretchr/testify/require"
)

func TestRevokeTokenAPIDisconnect(t *testing.T) {
	api, store, events := newAccessRevokeExecutor(t, config.DefaultConfig(), 100)

	resp := api.RevokeToken(context.Background(), &RevokeTokenRequest{
		User:       "user",
		Uid:        "session",
		RevokedAt:  1000,
		Disconnect: &Disconnect{Code: 4001, Reason: "session revoked"},
	})
	require.Nil(t, resp.Error)
	require.Equal(t, accessrevoke.Revoked, sessionVerdict(store, "session", 500))

	require.Len(t, *events, 1)
	notification, err := accessrevoke.DecodeNotification((*events)[0].Data)
	require.NoError(t, err)
	require.Equal(t, &accessrevoke.Disconnect{Code: 4001, Reason: "session revoked"}, notification.Disconnect)
}

func TestRevokeTokenAPIDisconnectRange(t *testing.T) {
	tests := []struct {
		name       string
		disconnect *Disconnect
		// want is the disconnect of the notification, nil when the request is bad.
		want *accessrevoke.Disconnect
	}{
		{name: "lowest code", disconnect: &Disconnect{Code: 4000}, want: &accessrevoke.Disconnect{Code: 4000}},
		{name: "highest code", disconnect: &Disconnect{Code: 4999}, want: &accessrevoke.Disconnect{Code: 4999}},
		{name: "code below range", disconnect: &Disconnect{Code: 3999}},
		{name: "code above range", disconnect: &Disconnect{Code: 5000}},
		{
			name:       "reason at limit",
			disconnect: &Disconnect{Code: 4001, Reason: strings.Repeat("r", 123)},
			want:       &accessrevoke.Disconnect{Code: 4001, Reason: strings.Repeat("r", 123)},
		},
		{name: "reason above limit", disconnect: &Disconnect{Code: 4001, Reason: strings.Repeat("r", 124)}},
		{name: "absent disconnect", disconnect: &Disconnect{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api, store, events := newAccessRevokeExecutor(t, config.DefaultConfig(), 100)

			resp := api.RevokeToken(context.Background(), &RevokeTokenRequest{
				User:       "user",
				Uid:        "session",
				RevokedAt:  1000,
				Disconnect: tt.disconnect,
			})
			if tt.want == nil && !isAbsent(tt.disconnect) {
				require.Equal(t, ErrorBadRequest, resp.Error)
				require.Equal(t, accessrevoke.Allowed, sessionVerdict(store, "session", 500))
				require.Empty(t, *events)
				return
			}
			require.Nil(t, resp.Error)
			require.Equal(t, accessrevoke.Revoked, sessionVerdict(store, "session", 500))
			require.Len(t, *events, 1)
			notification, err := accessrevoke.DecodeNotification((*events)[0].Data)
			require.NoError(t, err)
			require.Equal(t, tt.want, notification.Disconnect)
		})
	}
}

// isAbsent reports whether the request has no disconnect: code 0 with an empty reason.
func isAbsent(disconnect *Disconnect) bool {
	return disconnect.Code == 0 && disconnect.Reason == ""
}
