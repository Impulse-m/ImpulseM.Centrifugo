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
	revoker := NewRevoker(store, node.Hub(), queue)
	revoker.SetMaxTTL(func() time.Duration {
		return time.Hour
	})
	clampsBefore := clampedCount(t, ClampSourceNotify)

	farExpireAt := time.Now().Add(100 * 365 * 24 * time.Hour).Unix()
	before := time.Now().Unix()
	require.NoError(t, revoker.Apply(Notification{Kind: KindBlockUser, User: "user-1", ExpireAt: farExpireAt}, ClampSourceNotify))
	after := time.Now().Unix()

	require.Equal(t, clampsBefore+1, clampedCount(t, ClampSourceNotify))
	key, _ := Key("user-1")
	store.blockedMu.RLock()
	expireAt := store.blocked[key]
	store.blockedMu.RUnlock()
	// The clamp reads the clock once inside Apply, so the limit lies between the two reads plus the max TTL.
	require.GreaterOrEqual(t, expireAt, before+int64(time.Hour/time.Second))
	require.LessOrEqual(t, expireAt, after+int64(time.Hour/time.Second))
}

// TestRevokerApplyWithoutMaxTTLKeepsExpireAt applies a block with a far expire_at through a revoker with no limit, which
// is the default: the expire_at is kept as it is and nothing is counted.
func TestRevokerApplyWithoutMaxTTLKeepsExpireAt(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	revoker := NewRevoker(store, node.Hub(), NewDisconnectQueue(10000, 100))
	clampsBefore := clampedCount(t, ClampSourceNotify)

	farExpireAt := time.Now().Add(100 * 365 * 24 * time.Hour).Unix()
	require.NoError(t, revoker.Apply(Notification{Kind: KindBlockUser, User: "user-1", ExpireAt: farExpireAt}, ClampSourceNotify))

	require.Equal(t, clampsBefore, clampedCount(t, ClampSourceNotify))
	key, _ := Key("user-1")
	store.blockedMu.RLock()
	expireAt := store.blocked[key]
	store.blockedMu.RUnlock()
	require.Equal(t, farExpireAt, expireAt)
}
