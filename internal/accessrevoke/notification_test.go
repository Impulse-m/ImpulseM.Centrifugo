package accessrevoke

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

var allChecks = CheckOptions{Block: true, Session: true, UserTokens: true}

func TestNotificationEncodeDecode(t *testing.T) {
	notification := Notification{
		Kind:         KindInvalidateUserTokens,
		User:         "user",
		Generation:   7,
		IssuedBefore: 1000,
		RevokedAt:    1001,
		ExpireAt:     2000,
	}
	data, err := EncodeNotification(notification)
	require.NoError(t, err)

	decoded, err := DecodeNotification(data)
	require.NoError(t, err)
	require.Equal(t, notification, decoded)

	_, err = DecodeNotification([]byte("not json"))
	require.Error(t, err)
}

func TestApplyUnknownKind(t *testing.T) {
	node := tools.NodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	revoker := newTestRevoker(t, node, NewStore(time.Minute))

	err := revoker.Apply(Notification{Kind: "unknown"})
	require.ErrorIs(t, err, ErrUnknownKind)
}

func TestApplyBlockAndUnblock(t *testing.T) {
	node := tools.NodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	store := NewStore(time.Minute)
	revoker := newTestRevoker(t, node, store)

	err := revoker.Apply(Notification{Kind: KindBlockUser, User: "user", ExpireAt: time.Now().Add(time.Minute).Unix()})
	require.NoError(t, err)
	require.Equal(t, Blocked, store.Check("user", centrifuge.TokenIdentity{}, allChecks).Verdict)

	err = revoker.Apply(Notification{Kind: KindUnblockUser, User: "user"})
	require.NoError(t, err)
	require.Equal(t, Allowed, store.Check("user", centrifuge.TokenIdentity{}, allChecks).Verdict)
}

func TestApplyRevokeSession(t *testing.T) {
	node := tools.NodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	store := NewStore(time.Minute)
	revoker := newTestRevoker(t, node, store)

	err := revoker.Apply(Notification{
		Kind:      KindRevokeToken,
		Session:   "session",
		RevokedAt: 1000,
		ExpireAt:  time.Now().Add(time.Minute).Unix(),
	})
	require.NoError(t, err)
	result := store.Check("", Identity("session", 0, 999), allChecks)
	require.Equal(t, Revoked, result.Verdict)
}

func TestApplyInvalidateUser(t *testing.T) {
	node := tools.NodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	store := NewStore(time.Minute)
	revoker := newTestRevoker(t, node, store)

	err := revoker.Apply(Notification{
		Kind:       KindInvalidateUserTokens,
		User:       "user",
		Generation: 5,
		RevokedAt:  1000,
		ExpireAt:   time.Now().Add(time.Minute).Unix(),
	})
	require.NoError(t, err)
	require.Equal(t, Revoked, store.Check("user", Identity("s", 4, 500), allChecks).Verdict)
	require.Equal(t, Allowed, store.Check("user", Identity("s", 5, 500), allChecks).Verdict)
}

func TestNotifierAllowRate(t *testing.T) {
	node := tools.NodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	notifier := NewNotifier(node, 1)
	require.True(t, notifier.Allow())
	require.False(t, notifier.Allow())
}

func TestNotifierNotifyDeliversToHandler(t *testing.T) {
	node := tools.NodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	var events []centrifuge.NotificationEvent
	node.OnNotification(func(event centrifuge.NotificationEvent) {
		events = append(events, event)
	})

	notifier := NewNotifier(node, 100)
	err := notifier.Notify(Notification{Kind: KindUnblockUser, User: "user"})
	require.NoError(t, err)

	require.Len(t, events, 1)
	require.Equal(t, NotifyOp, events[0].Op)
	require.Equal(t, node.ID(), events[0].FromNodeID)

	notification, err := DecodeNotification(events[0].Data)
	require.NoError(t, err)
	require.Equal(t, Notification{Kind: KindUnblockUser, User: "user"}, notification)
}
