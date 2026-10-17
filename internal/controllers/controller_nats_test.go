//go:build integration

package controllers

import (
	"context"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// newNatsControlledNode returns a running node which coordinates with the other nodes of the prefix over Nats.
func newNatsControlledNode(t *testing.T, prefix string) (*centrifuge.Node, *NatsController) {
	t.Helper()
	node := newTestNode(t)
	controller, err := NewNatsController(node, configtypes.NatsController{
		NatsPrefixed: configtypes.NatsPrefixed{
			Prefix: prefix,
			NatsCommon: configtypes.NatsCommon{
				URL:          "nats://localhost:4222",
				DialTimeout:  configtypes.Duration(time.Second),
				WriteTimeout: configtypes.Duration(time.Second),
			},
		},
	})
	require.NoError(t, err)
	node.SetController(controller)
	startTestNode(t, node)
	return node, controller
}

func TestNatsControllerTwoNodes(t *testing.T) {
	ctx := context.Background()
	goroutinesBefore := runtime.NumGoroutine()
	prefix := "t-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	node1, controller1 := newNatsControlledNode(t, prefix)
	node2, controller2 := newNatsControlledNode(t, prefix)
	requireTwoNodes(t, node1, node2)

	// Disconnect is a control command: node1 has no such client, node2 must disconnect it.
	transport := connectTestClient(t, node2, "user-nats")
	require.NoError(t, node1.Disconnect("user-nats"))
	requireClosed(t, transport)

	require.NoError(t, node1.Shutdown(ctx))
	require.NoError(t, node2.Shutdown(ctx))
	require.NoError(t, controller1.Close(ctx))
	require.NoError(t, controller2.Close(ctx))
	requireGoroutinesAtMost(t, goroutinesBefore)
	t.Logf("goroutines before %d, after closing controllers %d", goroutinesBefore, runtime.NumGoroutine())
}
