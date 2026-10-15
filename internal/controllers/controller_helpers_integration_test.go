//go:build integration

package controllers

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// testTransport is a unidirectional transport which records when it is closed. It discards the data
// written to it, since the tests only check that a client is disconnected.
type testTransport struct {
	closed chan struct{}
}

var _ centrifuge.Transport = (*testTransport)(nil)

func newTestTransport() *testTransport {
	return &testTransport{closed: make(chan struct{})}
}

func (t *testTransport) Name() string {
	return "test"
}

func (t *testTransport) AcceptProtocol() string {
	return ""
}

func (t *testTransport) Protocol() centrifuge.ProtocolType {
	return centrifuge.ProtocolTypeJSON
}

func (t *testTransport) ProtocolVersion() centrifuge.ProtocolVersion {
	return centrifuge.ProtocolVersion2
}

func (t *testTransport) Unidirectional() bool {
	return true
}

func (t *testTransport) Emulation() bool {
	return false
}

func (t *testTransport) DisabledPushFlags() uint64 {
	return 0
}

func (t *testTransport) PingPongConfig() centrifuge.PingPongConfig {
	return centrifuge.PingPongConfig{PingInterval: -1}
}

func (t *testTransport) Write(_ []byte) error {
	return nil
}

func (t *testTransport) WriteMany(_ ...[]byte) error {
	return nil
}

func (t *testTransport) Close(_ centrifuge.Disconnect) error {
	select {
	case <-t.closed:
	default:
		close(t.closed)
	}
	return nil
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
