//go:build integration

package controllers

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// testTransport is a test transport which records when it is closed. The data written to it is discarded,
// since the tests only check that a client is disconnected.
type testTransport struct {
	*tools.TestTransport
	closed    chan struct{}
	closeOnce sync.Once
}

var _ centrifuge.Transport = (*testTransport)(nil)

func newTestTransport() *testTransport {
	return &testTransport{
		TestTransport: tools.NewTestTransport(),
		closed:        make(chan struct{}),
	}
}

// Unidirectional makes the client connect without sending commands, as the tests need.
func (t *testTransport) Unidirectional() bool {
	return true
}

// Close closes the transport and signals the test which waits for it.
func (t *testTransport) Close(disconnect centrifuge.Disconnect) error {
	err := t.TestTransport.Close(disconnect)
	t.closeOnce.Do(func() {
		close(t.closed)
	})
	return err
}

// newTestNode returns a node which is not running yet. The controller must be set on it before
// startTestNode is called.
func newTestNode(t *testing.T) *centrifuge.Node {
	t.Helper()
	node, err := centrifuge.New(centrifuge.Config{})
	require.NoError(t, err)
	return node
}

// startTestNode sets memory broker and presence manager, runs the node and shuts it down on cleanup.
// A connection authenticates with its token as the user ID.
func startTestNode(t *testing.T, node *centrifuge.Node) {
	t.Helper()
	broker, err := centrifuge.NewMemoryBroker(node, centrifuge.MemoryBrokerConfig{})
	require.NoError(t, err)
	presenceManager, err := centrifuge.NewMemoryPresenceManager(node, centrifuge.MemoryPresenceManagerConfig{})
	require.NoError(t, err)
	node.SetBroker(broker)
	node.SetPresenceManager(presenceManager)
	node.OnConnecting(func(_ context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{Credentials: &centrifuge.Credentials{UserID: e.Token}}, nil
	})
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
}

// connectTestClient connects a client with the user ID to the node and returns its transport.
func connectTestClient(t *testing.T, node *centrifuge.Node, userID string) *testTransport {
	t.Helper()
	transport := newTestTransport()
	client, _, err := centrifuge.NewClient(context.Background(), node, transport)
	require.NoError(t, err)
	client.Connect(centrifuge.ConnectRequest{Token: userID})
	require.Equal(t, userID, client.UserID())
	return transport
}

// requireClosed fails the test unless the transport is closed within the timeout.
func requireClosed(t *testing.T, transport *testTransport) {
	t.Helper()
	select {
	case <-transport.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("client on the other node was not disconnected")
	}
}

// requireTwoNodes waits until both nodes see the two nodes of the cluster. A node misses the node info
// published before its control subscription is up, so each node must see the other one before a test
// sends a control command.
func requireTwoNodes(t *testing.T, nodes ...*centrifuge.Node) {
	t.Helper()
	for _, node := range nodes {
		require.Eventually(t, func() bool {
			info, err := node.Info()
			return err == nil && len(info.Nodes) == 2
		}, 10*time.Second, 50*time.Millisecond)
	}
}

// requireGoroutinesAtMost waits for the number of goroutines to drop to the baseline within two seconds.
// It polls in the test goroutine: testify runs the condition of Eventually in a goroutine of its own, which
// would count itself.
func requireGoroutinesAtMost(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > baseline {
		if time.Now().After(deadline) {
			t.Fatalf("goroutines did not return to baseline %d, now %d", baseline, runtime.NumGoroutine())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// redisTestConfig is the Redis of the local docker-compose service.
func redisTestConfig() configtypes.Redis {
	return configtypes.Redis{
		Address:        []string{"redis://127.0.0.1:6379"},
		ConnectTimeout: configtypes.Duration(time.Second),
		IOTimeout:      configtypes.Duration(4 * time.Second),
	}
}
