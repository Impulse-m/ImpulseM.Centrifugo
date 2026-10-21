package client

import (
	"strings"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// A reply over the response bound is a recorded answer: its retransmit gets the same error, cached, and the
// backend runs once.
func TestRPCRegistryResponseTooLargeIsRecorded(t *testing.T) {
	backend := newDedupBackend(t)
	reg := dedupRegistryConfig()
	reg.Errors.ResponseTooLarge = configtypes.RPCRegistryError{Code: 1001, Message: "response too large"}
	reg.Methods = append(reg.Methods, configtypes.RPCMethod{
		Method:           "users.echo",
		GRPCMethod:       registryTestBackendMethod,
		Target:           "backend",
		MaxRequestBytes:  1024,
		MaxResponseBytes: 64,
	})
	h := newDedupHandler(t, reg, backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID := dedupMsgID(6)
	payload := strings.Repeat("x", 100)

	first := dedupCallAsyncResult(t, h, client, "users.echo", msgID, payload)
	protoErr := requireProtocolError(t, first.err)
	require.Equal(t, uint32(1001), protoErr.Code)
	require.False(t, protoErr.Temporary)
	require.False(t, first.reply.Cached)

	retransmit := dedupCallAsyncResult(t, h, client, "users.echo", msgID, payload)
	retransmitErr := requireProtocolError(t, retransmit.err)
	require.Equal(t, uint32(1001), retransmitErr.Code)
	require.True(t, retransmit.reply.Cached)
	require.Len(t, backend.Calls(), 1)
}
