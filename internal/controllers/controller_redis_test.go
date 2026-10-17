//go:build integration

package controllers

import (
	"context"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/confighelpers"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// newRedisControlledNode returns a running node which coordinates with the other nodes of the prefix over Redis.
func newRedisControlledNode(t *testing.T, prefix string) (*centrifuge.Node, *RedisController) {
	t.Helper()
	node := newTestNode(t)
	shards, _, err := confighelpers.CentrifugeRedisShards(node, redisTestConfig())
	require.NoError(t, err)
	controller, err := NewRedisController(node, shards, prefix)
	require.NoError(t, err)
	node.SetController(controller)
	startTestNode(t, node)
	return node, controller
}

func TestRedisControllerTwoNodes(t *testing.T) {
	ctx := context.Background()
	goroutinesBefore := runtime.NumGoroutine()
	prefix := "t-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	node1, controller1 := newRedisControlledNode(t, prefix)
	node2, controller2 := newRedisControlledNode(t, prefix)
	requireTwoNodes(t, node1, node2)

	// Disconnect is a control command: node1 has no such client, node2 must disconnect it.
	transport := connectTestClient(t, node2, "user-redis")
	require.NoError(t, node1.Disconnect("user-redis"))
	requireClosed(t, transport)

	require.NoError(t, node1.Shutdown(ctx))
	require.NoError(t, node2.Shutdown(ctx))
	require.NoError(t, controller1.Close(ctx))
	require.NoError(t, controller2.Close(ctx))
	requireGoroutinesAtMost(t, goroutinesBefore)
	t.Logf("goroutines before %d, after closing controllers %d", goroutinesBefore, runtime.NumGoroutine())
}
