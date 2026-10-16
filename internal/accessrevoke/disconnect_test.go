package accessrevoke

import (
	"strings"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

func TestValidateDisconnect(t *testing.T) {
	tests := []struct {
		name       string
		disconnect *Disconnect
		valid      bool
	}{
		{name: "nil", disconnect: nil, valid: true},
		{name: "absent code and reason", disconnect: &Disconnect{}, valid: true},
		{name: "lowest code", disconnect: &Disconnect{Code: 4000, Reason: "r"}, valid: true},
		{name: "highest code", disconnect: &Disconnect{Code: 4999, Reason: "r"}, valid: true},
		{name: "code below range", disconnect: &Disconnect{Code: 3999, Reason: "r"}, valid: false},
		{name: "code above range", disconnect: &Disconnect{Code: 5000, Reason: "r"}, valid: false},
		{name: "code without reason", disconnect: &Disconnect{Code: 4001}, valid: true},
		{name: "reason without code", disconnect: &Disconnect{Reason: "r"}, valid: false},
		{name: "reason at limit", disconnect: &Disconnect{Code: 4001, Reason: strings.Repeat("r", 123)}, valid: true},
		{name: "reason above limit", disconnect: &Disconnect{Code: 4001, Reason: strings.Repeat("r", 124)}, valid: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDisconnect(tt.disconnect)
			if tt.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestFromAPIDisconnect(t *testing.T) {
	require.Nil(t, FromAPIDisconnect(nil))
	require.Equal(t, &Disconnect{Code: 4001, Reason: "session revoked"}, FromAPIDisconnect(&apiproto.Disconnect{
		Code:   4001,
		Reason: "session revoked",
	}))
}

func TestCloseWith(t *testing.T) {
	require.Equal(t, centrifuge.DisconnectExpired, closeWith(nil, centrifuge.DisconnectExpired))
	require.Equal(t, centrifuge.Disconnect{Code: 4001, Reason: "r"}, closeWith(&Disconnect{Code: 4001, Reason: "r"}, centrifuge.DisconnectExpired))
}

func TestResolveRevokeTokenDisconnect(t *testing.T) {
	operation := Operation{Kind: KindRevokeToken, User: "u", Session: "s"}

	operation.Disconnect = &Disconnect{Code: 4001, Reason: "session revoked"}
	notification, err := Resolve(operation, resolveNow, resolveTTL)
	require.NoError(t, err)
	require.Equal(t, &Disconnect{Code: 4001, Reason: "session revoked"}, notification.Disconnect)

	operation.Disconnect = &Disconnect{}
	notification, err = Resolve(operation, resolveNow, resolveTTL)
	require.NoError(t, err)
	require.Nil(t, notification.Disconnect)

	operation.Disconnect = &Disconnect{Code: 3999, Reason: "r"}
	_, err = Resolve(operation, resolveNow, resolveTTL)
	var validationErr *ValidationError
	require.ErrorAs(t, err, &validationErr)
	require.False(t, validationErr.NotSupported)

	// Other kinds have no disconnect of their own.
	notification, err = Resolve(Operation{Kind: KindBlockUser, User: "u", Disconnect: &Disconnect{Code: 3999}}, resolveNow, resolveTTL)
	require.NoError(t, err)
	require.Nil(t, notification.Disconnect)
}

func TestNotificationDisconnectEncoding(t *testing.T) {
	withDisconnect := Notification{Kind: KindRevokeToken, Session: "s", Disconnect: &Disconnect{Code: 4001, Reason: "r"}}
	data, err := EncodeNotification(withDisconnect)
	require.NoError(t, err)
	require.JSONEq(t, `{"kind":"revoke_token","session":"s","disconnect":{"code":4001,"reason":"r"}}`, string(data))
	decoded, err := DecodeNotification(data)
	require.NoError(t, err)
	require.Equal(t, withDisconnect, decoded)

	// A notification without the field is encoded as before and decodes with no disconnect.
	withoutDisconnect := Notification{Kind: KindRevokeToken, Session: "s"}
	data, err = EncodeNotification(withoutDisconnect)
	require.NoError(t, err)
	require.JSONEq(t, `{"kind":"revoke_token","session":"s"}`, string(data))
	decoded, err = DecodeNotification([]byte(`{"kind":"revoke_token","session":"s"}`))
	require.NoError(t, err)
	require.Nil(t, decoded.Disconnect)
}

func TestDecodeOperationRevokeTokenDisconnect(t *testing.T) {
	operation, err := decodeOperation(KindRevokeToken, []byte(`{"uid":"s","user":"u","disconnect":{"code":4001,"reason":"r"}}`))
	require.NoError(t, err)
	require.Equal(t, &Disconnect{Code: 4001, Reason: "r"}, operation.Disconnect)

	// An invalid disconnect is dropped, and the revocation is still decoded with the default close.
	operation, err = decodeOperation(KindRevokeToken, []byte(`{"uid":"s","user":"u","revoked_at":10,"disconnect":{"code":5000}}`))
	require.NoError(t, err)
	require.Equal(t, Operation{Kind: KindRevokeToken, User: "u", Session: "s", RevokedAt: 10}, operation)

	operation, err = decodeOperation(KindRevokeToken, []byte(`{"uid":"s","user":"u","disconnect":{"code":-1}}`))
	require.NoError(t, err)
	require.Nil(t, operation.Disconnect)
	require.Equal(t, "s", operation.Session)
}

func TestRejectDisconnectRevokedUsesSessionDisconnect(t *testing.T) {
	fallback := centrifuge.Disconnect{Code: 3005, Reason: "connection expired"}

	disconnect, rejected := RejectDisconnect(Result{Verdict: Revoked, Reason: ReasonSession}, fallback)
	require.True(t, rejected)
	require.Equal(t, fallback, disconnect)

	disconnect, rejected = RejectDisconnect(Result{
		Verdict:    Revoked,
		Reason:     ReasonSession,
		Disconnect: &Disconnect{Code: 4001, Reason: "session revoked"},
	}, fallback)
	require.True(t, rejected)
	require.Equal(t, centrifuge.Disconnect{Code: 4001, Reason: "session revoked"}, disconnect)
}

func TestSanitizeRevokeTokenBody(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "valid disconnect is kept",
			body: `{"uid":"s","user":"u","disconnect":{"code":4001,"reason":"session revoked"}}`,
			want: `{"uid":"s","user":"u","disconnect":{"code":4001,"reason":"session revoked"}}`,
		},
		{
			name: "absent disconnect is kept",
			body: `{"uid":"s","user":"u"}`,
			want: `{"uid":"s","user":"u"}`,
		},
		{
			name: "null disconnect is kept",
			body: `{"uid":"s","disconnect":null}`,
			want: `{"uid":"s","disconnect":null}`,
		},
		{
			name: "code below range is removed",
			body: `{"uid":"s","user":"u","disconnect":{"code":3999,"reason":"r"}}`,
			want: `{"uid":"s","user":"u"}`,
		},
		{
			name: "negative code is removed",
			body: `{"uid":"s","disconnect":{"code":-1,"reason":"r"}}`,
			want: `{"uid":"s"}`,
		},
		{
			name: "string code is removed",
			body: `{"uid":"s","disconnect":{"code":"4001","reason":"r"}}`,
			want: `{"uid":"s"}`,
		},
		{
			name: "string disconnect is removed",
			body: `{"uid":"s","disconnect":"4001"}`,
			want: `{"uid":"s"}`,
		},
		{
			name: "key without case is removed",
			body: `{"uid":"s","DISCONNECT":{"code":5000,"reason":"r"}}`,
			want: `{"uid":"s"}`,
		},
		{
			name: "reason above limit is removed",
			body: `{"uid":"s","disconnect":{"code":4001,"reason":"` + strings.Repeat("r", 124) + `"}}`,
			want: `{"uid":"s"}`,
		},
		{
			name: "body which is not an object is returned as it is",
			body: `[1,2]`,
			want: `[1,2]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizeRevokeTokenBody([]byte(tt.body))
			require.JSONEq(t, tt.want, string(got))
		})
	}
}
