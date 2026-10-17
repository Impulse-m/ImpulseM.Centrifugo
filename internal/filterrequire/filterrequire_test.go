package filterrequire

import (
	"bytes"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/protocol"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestMissing(t *testing.T) {
	off := &configtypes.ChannelOptions{}
	on := &configtypes.ChannelOptions{RequireServerTagsFilter: true}
	filter := &protocol.FilterNode{Key: "type", Cmp: "eq", Val: "a"}

	require.False(t, Missing(off, nil))
	require.False(t, Missing(off, filter))
	require.False(t, Missing(on, filter))
	require.True(t, Missing(on, nil))
}

func TestSourceName(t *testing.T) {
	require.Equal(t, "connection_token", SourceName(subsource.ConnectionToken))
	require.Equal(t, "connect_proxy", SourceName(subsource.ConnectProxy))
	require.Equal(t, "subscription_token", SourceName(subsource.SubscriptionToken))
	require.Equal(t, "subscribe_proxy", SourceName(subsource.SubscribeProxy))
	require.Equal(t, "user_limited", SourceName(subsource.UserLimited))
	require.Equal(t, "client_allowed", SourceName(subsource.ClientAllowed))
	require.Equal(t, "client_insecure", SourceName(subsource.ClientInsecure))
	require.Equal(t, "uni_connect", SourceName(subsource.UniConnect))
	require.Equal(t, "user_personal", SourceName(subsource.UserPersonal))
	require.Equal(t, "server_api", SourceName(subsource.ServerAPI))
	require.Equal(t, "stream_proxy", SourceName(subsource.StreamProxy))
	require.Equal(t, "unknown", SourceName(subsource.Unknown))
}

// captureLog swaps the global logger for one which writes to the returned buffer, at the debug level. It does not run in
// parallel with other tests which log.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := log.Logger
	previousLevel := zerolog.GlobalLevel()
	log.Logger = zerolog.New(&buf).Level(zerolog.DebugLevel)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	t.Cleanup(func() {
		log.Logger = previous
		zerolog.SetGlobalLevel(previousLevel)
	})
	return &buf
}

func TestLogRefusedLevels(t *testing.T) {
	buf := captureLog(t)
	LogRefused("req:room", "c1", "12", subsource.SubscriptionToken)
	require.Contains(t, buf.String(), `"level":"warn"`)
	require.Contains(t, buf.String(), `"source":"subscription_token"`)
	require.Contains(t, buf.String(), `"channel":"req:room"`)
	require.Contains(t, buf.String(), "server tags filter required, subscription refused")

	buf.Reset()
	LogRefused("req:room", "c1", "12", subsource.UniConnect)
	require.NotContains(t, buf.String(), `"level":"warn"`)
	require.Contains(t, buf.String(), `"level":"debug"`)
	require.Contains(t, buf.String(), `"source":"uni_connect"`)
}

func TestLogHistoryRefused(t *testing.T) {
	buf := captureLog(t)
	LogHistoryRefused("req:room", "c1", "12")
	require.Contains(t, buf.String(), `"level":"debug"`)
	require.Contains(t, buf.String(), "server tags filter required, history refused")
}
