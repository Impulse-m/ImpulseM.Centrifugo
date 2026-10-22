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

func TestMapPublishOverrideOnlyKeepsPublishForOtherNamespaces(t *testing.T) {
	ops := configtypes.RateLimitOperations{
		Publish: configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Minute, 1)},
		MapPublish: configtypes.RateLimitChannelBuckets{
			Enabled: true,
			NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
				{NamespaceName: "chat", Enabled: true, Buckets: testList(time.Minute, 100)},
			},
		},
	}
	p := newCommandPlan(configtypes.RateLimitClientCommand{RateLimitOperations: ops})
	require.Same(t, p.ops[OperationPublish].base, p.resolve(OperationMapPublish, "news"))
	require.NotSame(t, p.ops[OperationPublish].base, p.resolve(OperationMapPublish, "chat"))

	// The publish budget of other namespaces is shared with plain publishes: the second map publish is refused.
	const now int64 = 1_000_000_000_000
	slots := make([]int64, p.numSlots)
	require.Zero(t, p.allow(slots, now, OperationMapPublish, "news"))
	require.Positive(t, p.allow(slots, now, OperationMapPublish, "news"))
	require.Positive(t, p.allow(slots, now, OperationPublish, "news"))
}

func TestInheritedOverridesKeepOwnOverrides(t *testing.T) {
	ops := configtypes.RateLimitOperations{
		Publish: configtypes.RateLimitChannelBuckets{
			Enabled: true,
			Buckets: testList(time.Minute, 1),
			NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
				{NamespaceName: "chat", Enabled: true, Buckets: testList(time.Minute, 100)},
			},
		},
		MapPublish: configtypes.RateLimitChannelBuckets{
			Enabled: true,
			NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
				{NamespaceName: "chat", Enabled: true, Buckets: testList(time.Minute, 2)},
				{NamespaceName: "news", Enabled: true, Buckets: testList(time.Minute, 5)},
			},
		},
	}
	p := newCommandPlan(configtypes.RateLimitClientCommand{RateLimitOperations: ops})
	require.Same(t, p.ops[OperationPublish].base, p.resolve(OperationMapPublish, "other"))
	require.Same(t, p.ops[OperationPublish].overrides["chat"], p.resolve(OperationPublish, "chat"))
	require.NotSame(t, p.ops[OperationPublish].overrides["chat"], p.resolve(OperationMapPublish, "chat"))
	require.NotSame(t, p.ops[OperationPublish].base, p.resolve(OperationMapPublish, "news"))
}
