package accessrevoke

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var resolveNow = time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)

const resolveTTL = 20 * time.Minute

func TestResolveBlockUserDefaults(t *testing.T) {
	notification, err := Resolve(Operation{Kind: KindBlockUser, User: "user"}, resolveNow, resolveTTL)
	require.NoError(t, err)
	require.Equal(t, Notification{
		Kind:     KindBlockUser,
		User:     "user",
		ExpireAt: resolveNow.Add(resolveTTL).Unix(),
	}, notification)
}

func TestResolveBlockUserExplicitExpireAt(t *testing.T) {
	notification, err := Resolve(Operation{Kind: KindBlockUser, User: "user", ExpireAt: 12345}, resolveNow, resolveTTL)
	require.NoError(t, err)
	require.Equal(t, int64(12345), notification.ExpireAt)
}

func TestResolveUnblockUserHasNoTimes(t *testing.T) {
	notification, err := Resolve(Operation{Kind: KindUnblockUser, User: "user"}, resolveNow, resolveTTL)
	require.NoError(t, err)
	require.Equal(t, Notification{Kind: KindUnblockUser, User: "user"}, notification)
}

func TestResolveRevokeTokenDefaults(t *testing.T) {
	notification, err := Resolve(Operation{Kind: KindRevokeToken, Session: "sid", User: "user"}, resolveNow, resolveTTL)
	require.NoError(t, err)
	require.Equal(t, Notification{
		Kind:      KindRevokeToken,
		User:      "user",
		Session:   "sid",
		RevokedAt: resolveNow.Unix(),
		ExpireAt:  resolveNow.Add(resolveTTL).Unix(),
	}, notification)
}

func TestResolveRevokeTokenExplicitTimes(t *testing.T) {
	notification, err := Resolve(Operation{
		Kind:      KindRevokeToken,
		Session:   "sid",
		RevokedAt: 100,
		ExpireAt:  200,
	}, resolveNow, resolveTTL)
	require.NoError(t, err)
	require.Empty(t, notification.User)
	require.Equal(t, int64(100), notification.RevokedAt)
	require.Equal(t, int64(200), notification.ExpireAt)
}

func TestResolveInvalidateUserTokensIssuedBeforeDefault(t *testing.T) {
	notification, err := Resolve(Operation{Kind: KindInvalidateUserTokens, User: "user"}, resolveNow, resolveTTL)
	require.NoError(t, err)
	require.Equal(t, resolveNow.Unix(), notification.IssuedBefore)
	require.Equal(t, resolveNow.Unix(), notification.RevokedAt)
	require.Equal(t, resolveNow.Add(resolveTTL).Unix(), notification.ExpireAt)
}

func TestResolveInvalidateUserTokensGenerationSkipsIssuedBeforeDefault(t *testing.T) {
	notification, err := Resolve(Operation{
		Kind:       KindInvalidateUserTokens,
		User:       "user",
		Generation: 3,
	}, resolveNow, resolveTTL)
	require.NoError(t, err)
	require.Equal(t, uint64(3), notification.Generation)
	require.Zero(t, notification.IssuedBefore)
}

func TestResolveInvalidateUserTokensExplicitIssuedBeforeWithGeneration(t *testing.T) {
	notification, err := Resolve(Operation{
		Kind:         KindInvalidateUserTokens,
		User:         "user",
		Generation:   3,
		IssuedBefore: 500,
	}, resolveNow, resolveTTL)
	require.NoError(t, err)
	require.Equal(t, int64(500), notification.IssuedBefore)
}

func TestResolveValidationErrors(t *testing.T) {
	tests := []struct {
		name         string
		operation    Operation
		notSupported bool
	}{
		{name: "block user without user", operation: Operation{Kind: KindBlockUser}},
		{name: "unblock user without user", operation: Operation{Kind: KindUnblockUser}},
		{name: "revoke token without uid", operation: Operation{Kind: KindRevokeToken, User: "user"}},
		{name: "invalidate without user", operation: Operation{Kind: KindInvalidateUserTokens}},
		{
			name:         "invalidate with channel is not supported",
			operation:    Operation{Kind: KindInvalidateUserTokens, User: "user", Channel: "chan"},
			notSupported: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Resolve(tt.operation, resolveNow, resolveTTL)
			var validationErr *ValidationError
			require.True(t, errors.As(err, &validationErr))
			require.Equal(t, tt.notSupported, validationErr.NotSupported)
			require.NotEmpty(t, validationErr.Error())
		})
	}
}

func TestResolveChannelCheckedBeforeUser(t *testing.T) {
	_, err := Resolve(Operation{Kind: KindInvalidateUserTokens, Channel: "chan"}, resolveNow, resolveTTL)
	var validationErr *ValidationError
	require.True(t, errors.As(err, &validationErr))
	require.True(t, validationErr.NotSupported)
}

func TestResolveUnknownKind(t *testing.T) {
	_, err := Resolve(Operation{Kind: "nope"}, resolveNow, resolveTTL)
	require.ErrorIs(t, err, ErrUnknownKind)
}
