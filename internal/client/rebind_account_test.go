package client

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// chainTestUUID is the session of the last refresh of the chain tests.
const chainTestUUID = "7c1f0e2d-3b4a-4c5d-8e6f-9a0b1c2d3e4f"

// accountStep is one refresh of a chain: the user and the class of its token. An empty user is an anonymous token.
type accountStep struct {
	toUser  string
	toClass string
}

// TestRebindAccountBindingOnEveryPath runs chains of refreshes over connections of each origin, and checks the account
// the connection holds after the chain and the result of its last refresh. The account is the user of the last token
// with a user, so a logout keeps it, and a connection with an account may only move to that account.
func TestRebindAccountBindingOnEveryPath(t *testing.T) {
	tests := []struct {
		name        string
		origin      string
		steps       []accountStep
		wantAccount string
		wantErr     error
	}{
		{
			name:        "token connection renews its user",
			origin:      "token",
			steps:       []accountStep{{toUser: "42", toClass: "authenticated"}},
			wantAccount: "42",
		},
		{
			name:        "token connection logs out and keeps its account",
			origin:      "token",
			steps:       []accountStep{{toClass: "anonymous"}},
			wantAccount: "42",
		},
		{
			name:        "token connection logs out and logs in to its account",
			origin:      "token",
			steps:       []accountStep{{toClass: "anonymous"}, {toUser: "42", toClass: "authenticated"}},
			wantAccount: "42",
		},
		{
			name:        "token connection logs out and may not log in to another user",
			origin:      "token",
			steps:       []accountStep{{toClass: "anonymous"}, {toUser: "43", toClass: "authenticated"}},
			wantAccount: "42",
			wantErr:     centrifuge.ErrorPermissionDenied,
		},
		{
			name:        "token connection may not move to another user",
			origin:      "token",
			steps:       []accountStep{{toUser: "43", toClass: "authenticated"}},
			wantAccount: "42",
			wantErr:     centrifuge.ErrorPermissionDenied,
		},
		{
			name:        "connect proxy connection renews its user",
			origin:      "proxy",
			steps:       []accountStep{{toUser: "42", toClass: "authenticated"}},
			wantAccount: "42",
		},
		{
			name:        "connect proxy connection may not log out",
			origin:      "proxy",
			steps:       []accountStep{{toClass: "anonymous"}},
			wantAccount: "42",
			wantErr:     centrifuge.ErrorPermissionDenied,
		},
		{
			name:        "connect proxy connection may not move to another user",
			origin:      "proxy",
			steps:       []accountStep{{toUser: "43", toClass: "authenticated"}},
			wantAccount: "42",
			wantErr:     centrifuge.ErrorPermissionDenied,
		},
		{
			name:        "anonymous connection takes any user",
			origin:      "anonymous",
			steps:       []accountStep{{toUser: "43", toClass: "authenticated"}},
			wantAccount: "43",
		},
		{
			name:        "anonymous connection keeps the account of its first user",
			origin:      "anonymous",
			steps:       []accountStep{{toUser: "42", toClass: "authenticated"}, {toClass: "anonymous"}, {toUser: "43", toClass: "authenticated"}},
			wantAccount: "42",
			wantErr:     centrifuge.ErrorPermissionDenied,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true, AllowDowngrade: true}, false)
			client, state := newAccountTestClient(tt.origin)

			var err error
			for _, step := range tt.steps {
				_, _, err = h.OnRefresh(client, centrifuge.RefreshEvent{
					Token: rebindTestToken(t, step.toUser, step.toClass, chainTestUUID, nil),
				}, nil)
			}
			if tt.wantErr != nil {
				require.Equal(t, tt.wantErr, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantAccount, state.record.identity.Load().account)
		})
	}
}

// newAccountTestClient returns a connection of origin, which holds the account 42 unless it is anonymous. The origin is
// token, proxy or anonymous, as the connect of such a connection leaves it.
func newAccountTestClient(origin string) (*rebindClientMock, *rebindTestState) {
	switch origin {
	case "token":
		return newRebindTestClientWithToken("42", "42", "authenticated")
	case "proxy":
		return newRebindTestClient("42", "")
	}
	client, state := newRebindTestClientWithToken("", "", "")
	state.record.identity.Store(nextIdentity(nil, originAnonymous, 0, rebindTestClaim, "", jwtverify.ConnectToken{}))
	return client, state
}

// TestConnectProxyConnectionMayNotRebind checks that a connection made through a connect proxy answers any change of
// its identity with a permission error, whether it has a user or not, and that a same-user renewal is not a change.
func TestConnectProxyConnectionMayNotRebind(t *testing.T) {
	tests := []struct {
		name     string
		user     string
		toUser   string
		toClass  string
		wantErr  error
		wantUser string
	}{
		{name: "user proxy to another user", user: "42", toUser: "43", toClass: "authenticated", wantErr: centrifuge.ErrorPermissionDenied, wantUser: "42"},
		{name: "user proxy to anonymous", user: "42", toClass: "anonymous", wantErr: centrifuge.ErrorPermissionDenied, wantUser: "42"},
		{name: "anonymous proxy to a user", user: "", toUser: "42", toClass: "authenticated", wantErr: centrifuge.ErrorPermissionDenied, wantUser: ""},
		{name: "anonymous proxy to anonymous", user: "", toClass: "anonymous"},
		{name: "user proxy renewal of its user", user: "42", toUser: "42", toClass: "authenticated", wantUser: "42"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true, AllowDowngrade: true}, false)
			client, state := newRebindTestClient(tt.user, "")

			_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
				Token: rebindTestToken(t, tt.toUser, tt.toClass, chainTestUUID, nil),
			}, nil)
			if tt.wantErr != nil {
				require.Equal(t, tt.wantErr, err)
			} else {
				require.NoError(t, err)
			}
			require.Empty(t, state.rebinds)
			require.Equal(t, tt.wantUser, state.user)
		})
	}
}

// TestSameUserClassChangeDenied checks that a same-user change between authenticated and bot is refused: the class of a
// connection changes only through the transition table, and this change is none of its transitions.
func TestSameUserClassChangeDenied(t *testing.T) {
	tests := []struct {
		name      string
		fromClass string
		toClass   string
	}{
		{name: "authenticated to bot", fromClass: "authenticated", toClass: "bot"},
		{name: "bot to authenticated", fromClass: "bot", toClass: "authenticated"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newRebindTestHandler(t, configtypes.Rebind{Enabled: true}, false)
			client, state := newRebindTestClientWithToken("42", "42", tt.fromClass)

			_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
				Token: rebindTestToken(t, "42", tt.toClass, chainTestUUID, nil),
			}, nil)
			require.Equal(t, centrifuge.ErrorPermissionDenied, err)
			require.Empty(t, state.rebinds)
			require.Equal(t, registryTestClass(tt.fromClass), state.record.identity.Load().class)
		})
	}
}
