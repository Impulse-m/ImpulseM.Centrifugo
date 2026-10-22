package accessrevoke

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRevokerApplyClampsToMaxTTL applies a block with a far expire_at through a revoker which has a max TTL of one hour.
// The block is kept until now plus the max TTL, and the clamp is counted under the notify source.
func TestRevokerApplyClampsToMaxTTL(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	queue := NewDisconnectQueue(10000, 100)
	revoker := NewRevokerWithMaxTTL(store, node.Hub(), queue, func() time.Duration {
		return time.Hour
	})
	clampsBefore := clampedCount(t, ClampSourceNotify)

	farExpireAt := time.Now().Add(100 * 365 * 24 * time.Hour).Unix()
	require.NoError(t, revoker.Apply(Notification{Kind: KindBlockUser, User: "user-1", ExpireAt: farExpireAt}))

	require.Equal(t, clampsBefore+1, clampedCount(t, ClampSourceNotify))
	key, _ := Key("user-1")
	store.blockedMu.RLock()
	expireAt := store.blocked[key]
	store.blockedMu.RUnlock()
	require.InDelta(t, time.Now().Add(time.Hour).Unix(), expireAt, 5)
}

// TestRevokerApplyWithoutMaxTTLKeepsExpireAt applies a block with a far expire_at through a revoker with no limit, which
// is the default: the expire_at is kept as it is and nothing is counted.
func TestRevokerApplyWithoutMaxTTLKeepsExpireAt(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	revoker := NewRevokerWithMaxTTL(store, node.Hub(), NewDisconnectQueue(10000, 100), nil)
	clampsBefore := clampedCount(t, ClampSourceNotify)

	farExpireAt := time.Now().Add(100 * 365 * 24 * time.Hour).Unix()
	require.NoError(t, revoker.Apply(Notification{Kind: KindBlockUser, User: "user-1", ExpireAt: farExpireAt}))

	require.Equal(t, clampsBefore, clampedCount(t, ClampSourceNotify))
	key, _ := Key("user-1")
	store.blockedMu.RLock()
	expireAt := store.blocked[key]
	store.blockedMu.RUnlock()
	require.Equal(t, farExpireAt, expireAt)
}
