package config

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestValidatePongAtWrite(t *testing.T) {
	tests := []struct {
		name         string
		on           bool
		pongTimeout  time.Duration
		writeTimeout time.Duration
		wantErr      string
	}{
		{
			name:         "off with a zero write timeout",
			on:           false,
			pongTimeout:  8 * time.Second,
			writeTimeout: 0,
		},
		{
			name:         "on with a positive write timeout",
			on:           true,
			pongTimeout:  8 * time.Second,
			writeTimeout: time.Second,
		},
		{
			name:         "on with a zero write timeout",
			on:           true,
			pongTimeout:  8 * time.Second,
			writeTimeout: 0,
			wantErr:      "client.pong_timeout_at_write: websocket.write_timeout must be positive when it is on, got 0s",
		},
		{
			name:         "on with a negative write timeout",
			on:           true,
			pongTimeout:  8 * time.Second,
			writeTimeout: -time.Millisecond,
			wantErr:      "client.pong_timeout_at_write: websocket.write_timeout must be positive when it is on, got -1ms",
		},
		{
			name:         "on with pong checks off and a negative write timeout",
			on:           true,
			pongTimeout:  -1,
			writeTimeout: -time.Millisecond,
		},
		{
			name:         "on with pong checks off at -1s",
			on:           true,
			pongTimeout:  -time.Second,
			writeTimeout: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Client.PongTimeoutAtWrite = tt.on
			cfg.Client.PongTimeout = configtypes.Duration(tt.pongTimeout)
			cfg.WebSocket.WriteTimeout = configtypes.Duration(tt.writeTimeout)
			err := validatePongAtWrite(cfg)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestClientPongAtWriteDefaultsOff(t *testing.T) {
	cfg := DefaultConfig()
	require.False(t, cfg.Client.PongTimeoutAtWrite)
}
