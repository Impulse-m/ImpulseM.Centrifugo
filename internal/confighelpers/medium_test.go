package confighelpers

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestMediumOptionsFromChannelOptionsKeepLatestPublication(t *testing.T) {
	opts := MediumOptionsFromChannelOptions(configtypes.ChannelOptions{
		KeepLatestPublication: true,
	})
	require.True(t, opts.KeepLatestPublication)
	require.False(t, opts.SharedPositionSync)
}

func TestMediumEnabled(t *testing.T) {
	tests := []struct {
		name     string
		chOpts   configtypes.ChannelOptions
		expected bool
	}{
		{
			name:     "nothing enabled",
			chOpts:   configtypes.ChannelOptions{},
			expected: false,
		},
		{
			name: "shared position sync enabled",
			chOpts: configtypes.ChannelOptions{
				SharedPositionSync: true,
			},
			expected: true,
		},
		{
			name: "keep latest publication enabled",
			chOpts: configtypes.ChannelOptions{
				KeepLatestPublication: true,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, MediumEnabled(tt.chOpts))
		})
	}
}

func TestMediumOptionsFromChannelOptions(t *testing.T) {
	tests := []struct {
		name     string
		chOpts   configtypes.ChannelOptions
		expected bool
	}{
		{
			name:     "shared position sync disabled",
			chOpts:   configtypes.ChannelOptions{},
			expected: false,
		},
		{
			name: "shared position sync enabled",
			chOpts: configtypes.ChannelOptions{
				SharedPositionSync: true,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := MediumOptionsFromChannelOptions(tt.chOpts)
			require.Equal(t, tt.expected, opts.SharedPositionSync)
		})
	}
}
