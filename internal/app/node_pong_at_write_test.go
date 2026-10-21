package app

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/stretchr/testify/require"
)

// TestCentrifugeNodeConfigPongAtWrite checks that client.pong_timeout_at_write reaches the node, which starts the pong
// timeout of every connection at the write of its ping.
func TestCentrifugeNodeConfigPongAtWrite(t *testing.T) {
	tests := []struct {
		name string
		on   bool
	}{
		{name: "off", on: false},
		{name: "on", on: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.Client.PongTimeoutAtWrite = tt.on
			cfgContainer, err := config.NewContainer(cfg)
			require.NoError(t, err)

			nodeCfg := centrifugeNodeConfig("test", "oss", cfgContainer, nil)
			require.Equal(t, tt.on, nodeCfg.ClientPongAtWrite)
		})
	}
}
