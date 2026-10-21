package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMaxMessagesInFrameDefault checks that a config loaded without client.max_messages_in_frame has the default 16.
func TestMaxMessagesInFrameDefault(t *testing.T) {
	path := writeConfigFile(t, "config.json", "{}")
	conf, _, err := GetConfig(nil, path)
	require.NoError(t, err)
	require.Equal(t, 16, conf.Client.MaxMessagesInFrame)
}
