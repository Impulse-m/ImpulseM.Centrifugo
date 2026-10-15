package config

import (
	"math"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

func TestValidateBandwidthOptions(t *testing.T) {
	tests := []struct {
		name    string
		opts    configtypes.ChannelOptions
		wantErr string
	}{
		{
			name: "keep latest publication without delta types",
			opts: configtypes.ChannelOptions{
				KeepLatestPublication: true,
			},
			wantErr: "keep_latest_publication requires allowed_delta_types",
		},
		{
			name: "keep latest publication with fossil",
			opts: configtypes.ChannelOptions{
				KeepLatestPublication: true,
				AllowedDeltaTypes:     []centrifuge.DeltaType{centrifuge.DeltaTypeFossil},
			},
		},
		{
			name: "negative debounce interval",
			opts: configtypes.ChannelOptions{
				ClientPublishDebounceInterval: configtypes.Duration(-time.Millisecond),
			},
			wantErr: "client_publish_debounce_interval must not be negative",
		},
		{
			name: "sub-millisecond debounce interval",
			opts: configtypes.ChannelOptions{
				ClientPublishDebounceInterval: configtypes.Duration(500 * time.Microsecond),
			},
			wantErr: "client_publish_debounce_interval must be a whole number of milliseconds, at least 1ms",
		},
		{
			name: "fractional millisecond debounce interval",
			opts: configtypes.ChannelOptions{
				ClientPublishDebounceInterval: configtypes.Duration(1500 * time.Microsecond),
			},
			wantErr: "client_publish_debounce_interval must be a whole number of milliseconds, at least 1ms",
		},
		{
			name: "debounce interval too large",
			opts: configtypes.ChannelOptions{
				ClientPublishDebounceInterval: configtypes.Duration((math.MaxUint32 + 1) * time.Millisecond),
			},
			wantErr: "client_publish_debounce_interval is too large",
		},
		{
			name: "largest debounce interval",
			opts: configtypes.ChannelOptions{
				ClientPublishDebounceInterval: configtypes.Duration(math.MaxUint32 * time.Millisecond),
			},
		},
		{
			name: "zero debounce interval",
			opts: configtypes.ChannelOptions{
				ClientPublishDebounceInterval: 0,
			},
		},
		{
			name: "one millisecond debounce interval",
			opts: configtypes.ChannelOptions{
				ClientPublishDebounceInterval: configtypes.Duration(time.Millisecond),
			},
		},
		{
			name: "fifty millisecond debounce interval",
			opts: configtypes.ChannelOptions{
				ClientPublishDebounceInterval: configtypes.Duration(50 * time.Millisecond),
			},
		},
		{
			name: "channel compaction alone",
			opts: configtypes.ChannelOptions{
				AllowChannelCompaction: true,
			},
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
			})
		}
	}
}
