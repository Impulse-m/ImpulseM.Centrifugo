package ratelimit

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestMapOperationsInheritPublishBuckets(t *testing.T) {
	ops := configtypes.RateLimitOperations{
		Publish:    configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Minute, 1)},
		SubRefresh: configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Minute, 2)},
	}
	p := newCommandPlan(configtypes.RateLimitClientCommand{RateLimitOperations: ops})
	require.NotNil(t, p.ops[OperationPublish].base)
	require.Same(t, p.ops[OperationPublish].base, p.ops[OperationMapPublish].base)
	require.Same(t, p.ops[OperationPublish].base, p.ops[OperationMapRemove].base)
	require.Same(t, p.ops[OperationSubRefresh].base, p.ops[OperationTrack].base)
	require.Same(t, p.ops[OperationSubRefresh].base, p.ops[OperationUntrack].base)

	// Map operations with their own buckets keep them.
	ops.MapPublish = configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Minute, 3)}
	p = newCommandPlan(configtypes.RateLimitClientCommand{RateLimitOperations: ops})
	require.NotSame(t, p.ops[OperationPublish].base, p.ops[OperationMapPublish].base)
	require.Same(t, p.ops[OperationPublish].base, p.ops[OperationMapRemove].base)
}
