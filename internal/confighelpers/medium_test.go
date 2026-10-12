package confighelpers

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

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
