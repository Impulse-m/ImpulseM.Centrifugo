//go:build integration

package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
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

// unsubscribePushTransport records the unsubscribe pushes written to a client.
type unsubscribePushTransport struct {
	*testTransport
	mu     sync.Mutex
	pushes []*protocol.Unsubscribe
}

// Write records the unsubscribe pushes of the message.
func (t *unsubscribePushTransport) Write(message []byte) error {
	return t.WriteMany(message)
}

// WriteMany records the unsubscribe pushes of the messages. A push is written as a bare push object, not wrapped in
// a reply, so each object is decoded as a push.
func (t *unsubscribePushTransport) WriteMany(messages ...[]byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, message := range messages {
		decoder := json.NewDecoder(bytes.NewReader(message))
		for {
			var push protocol.Push
			err := decoder.Decode(&push)
			if err != nil {
				break
			}
			if push.Unsubscribe != nil {
				t.pushes = append(t.pushes, push.Unsubscribe)
			}
		}
	}
	return nil
}

// unsubscribePushes returns the unsubscribe pushes received so far.
func (t *unsubscribePushTransport) unsubscribePushes() []*protocol.Unsubscribe {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]*protocol.Unsubscribe(nil), t.pushes...)
}

func TestNatsControllerUnsubscribeCodeTwoNodes(t *testing.T) {
	ctx := context.Background()
	prefix := "t-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	node1, controller1 := newNatsControlledNode(t, prefix)
	node2, controller2 := newNatsControlledNode(t, prefix)
	requireTwoNodes(t, node1, node2)

	// The client is connected to node2 only. node1 unsubscribes it with a resubscribe code, so node2 must send the
	// client the code and reason of the unsubscribe.
	transport := &unsubscribePushTransport{testTransport: newTestTransport()}
	client, _, err := centrifuge.NewClient(ctx, node2, transport)
	require.NoError(t, err)
	client.Connect(centrifuge.ConnectRequest{Token: "user-nats"})
	require.Equal(t, "user-nats", client.UserID())
	require.NoError(t, node2.Subscribe("user-nats", "unsub:room"))
	// node2 must hold the subscription before node1 unsubscribes it, so a failed subscribe is not taken for a lost
	// control message.
	require.Equal(t, 1, node2.Hub().NumSubscribers("unsub:room"), "node2 holds no subscriber of the channel before the unsubscribe")

	require.NoError(t, node1.Unsubscribe("user-nats", "unsub:room", centrifuge.WithCustomUnsubscribe(centrifuge.Unsubscribe{
		Code:   2600,
		Reason: "resubscribe",
	})))
	require.Eventually(t, func() bool {
		return len(transport.unsubscribePushes()) == 1
	}, 5*time.Second, 10*time.Millisecond, "node2 sent no unsubscribe push for the unsubscribe control message of node1")
	push := transport.unsubscribePushes()[0]
	require.Equal(t, uint32(2600), push.Code)
	require.Equal(t, "resubscribe", push.Reason)

	require.NoError(t, node1.Shutdown(ctx))
	require.NoError(t, node2.Shutdown(ctx))
	require.NoError(t, controller1.Close(ctx))
	require.NoError(t, controller2.Close(ctx))
}
