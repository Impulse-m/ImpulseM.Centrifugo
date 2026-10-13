package config

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestValidateSharedPositionSync(t *testing.T) {
	tests := []struct {
		name       string
		opts       configtypes.ChannelOptions
		checkDelay configtypes.Duration
		wantErr    string
	}{
		{
			name: "without positioning or recovery",
			opts: configtypes.ChannelOptions{
				SharedPositionSync: true,
			},
			checkDelay: configtypes.Duration(40 * time.Second),
			wantErr:    "shared_position_sync requires positioning or recovery",
		},
		{
			name: "zero channel position check delay",
			opts: configtypes.ChannelOptions{
				SharedPositionSync: true,
				ForcePositioning:   true,
			},
			checkDelay: 0,
			wantErr:    "shared_position_sync requires client.channel_position_check_delay to be greater than zero",
		},
		{
			name: "valid with force positioning",
			opts: configtypes.ChannelOptions{
				SharedPositionSync: true,
				ForcePositioning:   true,
			},
			checkDelay: configtypes.Duration(40 * time.Second),
		},
		{
			name: "valid with allow recovery",
			opts: configtypes.ChannelOptions{
				SharedPositionSync: true,
				AllowRecovery:      true,
			},
			checkDelay: configtypes.Duration(40 * time.Second),
		},
	}

	for _, tt := range tests {
		for _, inNamespace := range []bool{true, false} {
			name := tt.name + " without namespace"
			if inNamespace {
				name = tt.name + " in namespace"
			}
			t.Run(name, func(t *testing.T) {
				cfg := DefaultConfig()
				cfg.Client.ChannelPositionCheckDelay = tt.checkDelay
				if inNamespace {
					cfg.Channel.Namespaces = []configtypes.ChannelNamespace{
						{
							Name:           "test",
							ChannelOptions: tt.opts,
						},
					}
				} else {
					cfg.Channel.WithoutNamespace = tt.opts
				}
				err := cfg.Validate()
				if tt.wantErr == "" {
					require.NoError(t, err)
					return
				}
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				if inNamespace {
					require.Contains(t, err.Error(), "namespace test")
				}
			})
		}
	}
}
