package ratelimit

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestUnlimitedFallbacks(t *testing.T) {
	publishOnly := configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Minute, 1)}
	subRefreshOnly := configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(time.Minute, 1)}
	overrideOnly := configtypes.RateLimitChannelBuckets{
		NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
			{
				NamespaceName: "ns",
				Enabled:       true,
				Buckets:       testList(time.Minute, 1),
			},
		},
	}

	tests := []struct {
		name     string
		ops      configtypes.RateLimitOperations
		expected []Operation
	}{
		{
			name:     "no publish or sub refresh limit",
			ops:      configtypes.RateLimitOperations{},
			expected: nil,
		},
		{
			name: "default covers the fallback",
			ops: configtypes.RateLimitOperations{
				Default: testContainer(time.Minute, 1),
				Publish: publishOnly,
			},
			expected: nil,
		},
		{
			name: "publish without map buckets",
			ops: configtypes.RateLimitOperations{
				Publish: publishOnly,
			},
			expected: []Operation{OperationMapPublish, OperationMapRemove},
		},
		{
			name: "publish namespace override counts as a publish limit",
			ops: configtypes.RateLimitOperations{
				Publish: overrideOnly,
			},
			expected: []Operation{OperationMapPublish, OperationMapRemove},
		},
		{
			name: "map publish set leaves map remove",
			ops: configtypes.RateLimitOperations{
				Publish:    publishOnly,
				MapPublish: publishOnly,
			},
			expected: []Operation{OperationMapRemove},
		},
		{
			name: "sub refresh without track buckets",
			ops: configtypes.RateLimitOperations{
				SubRefresh: subRefreshOnly,
			},
			expected: []Operation{OperationTrack, OperationUntrack},
		},
		{
			name: "disabled publish limit",
			ops: configtypes.RateLimitOperations{
				Publish: configtypes.RateLimitChannelBuckets{Enabled: false, Buckets: testList(time.Minute, 1)},
			},
			expected: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, UnlimitedFallbacks(&tt.ops))
		})
	}
}
