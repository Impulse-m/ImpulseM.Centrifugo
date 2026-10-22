package ratelimit

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRefusalCacheExpiresAndRemovesEntries(t *testing.T) {
	c := newRefusalCache()
	c.refuse("key", 1000, 500*time.Nanosecond)

	wait, ok := c.refused("key", 1200)
	require.True(t, ok)
	require.Equal(t, 300*time.Nanosecond, wait)

	_, ok = c.refused("key", 1500)
	require.False(t, ok)
	require.Empty(t, c.shardOf("key").ends)
}

func TestRefusalCacheIgnoresNoWaitAndKeepsLaterEnd(t *testing.T) {
	c := newRefusalCache()
	c.refuse("key", 1000, 0)
	_, ok := c.refused("key", 1000)
	require.False(t, ok)

	c.refuse("key", 1000, 800*time.Nanosecond)
	c.refuse("key", 1000, 300*time.Nanosecond)
	wait, ok := c.refused("key", 1000)
	require.True(t, ok)
	require.Equal(t, 800*time.Nanosecond, wait)
}

func TestRefusalCacheSkipsInsertIntoFullShard(t *testing.T) {
	c := newRefusalCache()
	c.perShard = 2
	shard := c.shardOf("a")

	// Keys of the same shard fill it, a third one is not cached while the others are still there.
	var sameShard []string
	for i := 0; len(sameShard) < 3; i++ {
		key := fmt.Sprintf("k%d", i)
		if c.shardOf(key) == shard {
			sameShard = append(sameShard, key)
		}
	}
	c.refuse(sameShard[0], 0, time.Hour)
	c.refuse(sameShard[1], 0, time.Hour)
	c.refuse(sameShard[2], 0, time.Hour)
	require.Len(t, shard.ends, 2)
	_, ok := c.refused(sameShard[2], 1)
	require.False(t, ok)
}
