package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClientPongAtWriteDefaultsOff(t *testing.T) {
	cfg := DefaultConfig()
	require.False(t, cfg.Client.PongTimeoutAtWrite)
}
