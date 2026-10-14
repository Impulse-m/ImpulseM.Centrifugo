package redisshard

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJumpConsistentHashSingleBucket(t *testing.T) {
	for i := 0; i < 1000; i++ {
		require.Equal(t, int32(0), jumpConsistentHash(fnv1a64(fmt.Sprint("user-", i)), 1))
	}
}


func TestShardIndexDistribution(t *testing.T) {
	const numUsers = 30000
	const numShards = 3
	counts := make([]int, numShards)
	for i := 0; i < numUsers; i++ {
		idx := ShardIndex(fmt.Sprint("user-", i), numShards)
		require.GreaterOrEqual(t, idx, 0)
		require.Less(t, idx, numShards)
		counts[idx]++
	}
	expected := float64(numUsers) / numShards
	for _, c := range counts {
		require.InDelta(t, expected, float64(c), expected*0.1)
	}
}


func TestShardIndexStableAndMinimalMovement(t *testing.T) {
	const numUsers = 20000
	moved := 0
	for i := 0; i < numUsers; i++ {
		userID := fmt.Sprint("user-", i)
		before := ShardIndex(userID, 3)
		require.Equal(t, before, ShardIndex(userID, 3))
		after := ShardIndex(userID, 4)
		if before != after {
			// Jump hash moves a key only to the new bucket.
			require.Equal(t, 3, after)
			moved++
		}
	}
	// About a quarter of the users move to the new shard.
	require.InDelta(t, numUsers/4, moved, numUsers*0.03)
}


func TestShardIndexSingleShard(t *testing.T) {
	require.Equal(t, 0, ShardIndex("user-1", 0))
	require.Equal(t, 0, ShardIndex("user-1", 1))
}


func TestFnv1a64(t *testing.T) {
	// Reference values of FNV-1a 64.
	require.Equal(t, uint64(0xcbf29ce484222325), fnv1a64(""))
	require.Equal(t, uint64(0xaf63dc4c8601ec8c), fnv1a64("a"))
}
