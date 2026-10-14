package client

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/clientstorage"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

// dedupBackend is an in-process gRPC backend for the exactly-once tests. It answers "reply:" with the request
// bytes. A call whose request is "gate" is held until the test releases the gate, or until the call ends.
type dedupBackend struct {
	listener    *bufconn.Listener
	server      *grpc.Server
	entered     chan struct{}
	gate        chan struct{}
	releaseOnce sync.Once
	mu          sync.Mutex
	calls       []tools.GRPCBytesCall
}

// newDedupBackend starts a backend, which is stopped when the test ends.
func newDedupBackend(t testing.TB) *dedupBackend {
	t.Helper()
	b := &dedupBackend{
		listener: bufconn.Listen(1 << 20),
		entered:  make(chan struct{}, 64),
		gate:     make(chan struct{}),
	}
	b.server = grpc.NewServer(
		grpc.ForceServerCodec(tools.GRPCBytesCodec{}),
		grpc.UnknownServiceHandler(b.handle),
	)
	go func() {
		_ = b.server.Serve(b.listener)
	}()
	t.Cleanup(b.server.Stop)
	return b
}

// handle records the call, and answers it. A gated call waits for the gate or for its own end.
func (b *dedupBackend) handle(_ any, stream grpc.ServerStream) error {
	method, _ := grpc.MethodFromServerStream(stream)
	md, _ := metadata.FromIncomingContext(stream.Context())
	var in []byte
	if err := stream.RecvMsg(&in); err != nil {
		return err
	}
	b.mu.Lock()
	b.calls = append(b.calls, tools.GRPCBytesCall{Method: method, Metadata: md})
	b.mu.Unlock()
	b.entered <- struct{}{}
	// Clients send JSON, so a payload of the JSON string "gate" is a gated call too.
	if string(in) == "gate" || string(in) == `"gate"` {
		select {
		case <-b.gate:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	// A JSON string is answered as it is: the JSON protocol needs valid JSON replies.
	out := append([]byte("reply:"), in...)
	if len(in) > 0 && in[0] == '"' {
		out = in
	}
	return stream.SendMsg(&out)
}

// conn returns a client connection to the backend, closed when the test ends.
func (b *dedupBackend) conn(t testing.TB) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(
		"passthrough:///dedup-backend",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return b.listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})
	return conn
}

// waitEntered waits until the backend received a call.
func (b *dedupBackend) waitEntered(t testing.TB) {
	t.Helper()
	select {
	case <-b.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("backend call did not start")
	}
}

// releaseGate lets the gated calls finish.
func (b *dedupBackend) releaseGate() {
	b.releaseOnce.Do(func() {
		close(b.gate)
	})
}

// Calls returns the calls received so far.
func (b *dedupBackend) Calls() []tools.GRPCBytesCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]tools.GRPCBytesCall(nil), b.calls...)
}

// dedupRegistryConfig returns the registry of the exactly-once tests: an authenticated non-idempotent method,
// an authenticated idempotent one, an anonymous non-idempotent one and a second non-idempotent method of
// another name. The dedup is on, with the protocol errors of the store.
func dedupRegistryConfig() configtypes.RPCRegistry {
	reg := registryTestRegistryConfig()
	idempotent := true
	reg.Methods = append(reg.Methods,
		configtypes.RPCMethod{Method: "users.read", GRPCMethod: registryTestBackendMethod, Target: "backend", Idempotent: &idempotent},
		configtypes.RPCMethod{Method: "users.put", GRPCMethod: registryTestBackendMethod, Target: "backend"},
	)
	reg.Errors.MsgIDInvalid = configtypes.RPCRegistryError{Code: 107, Message: "invalid msg_id"}
	reg.Errors.MsgIDReused = configtypes.RPCRegistryError{Code: 107, Message: "msg_id reused"}
	reg.Errors.ResultEvicted = configtypes.RPCRegistryError{Code: 110, Message: "result expired"}
	reg.Errors.DedupUnavailable = configtypes.RPCRegistryError{Code: 108, Message: "dedup unavailable"}
	reg.Dedup = configtypes.RPCDedup{
		Enabled:         true,
		Window:          configtypes.Duration(120 * time.Second),
		MaxSkew:         configtypes.Duration(30 * time.Second),
		MaxReplySize:    65536,
		LocalMaxEntries: 1000,
		OnStoreError:    configtypes.RPCDedupFailClosed,
	}
	return reg
}

// startDedupRegistry starts the registry of reg, whose target uses conn.
func startDedupRegistry(t testing.TB, reg configtypes.RPCRegistry, conn *grpc.ClientConn) *rpcregistry.Registry {
	t.Helper()
	dial := func(*rpcregistry.Target) (io.Closer, error) {
		return conn, nil
	}
	registry, err := rpcregistry.Start(reg, dial)
	require.NoError(t, err)
	return registry
}

// newDedupHandler returns a handler of reg, whose registry calls backend.
func newDedupHandler(t testing.TB, reg configtypes.RPCRegistry, backend *dedupBackend) *Handler {
	t.Helper()
	registry := startDedupRegistry(t, reg, backend.conn(t))
	cfg := config.DefaultConfig()
	cfg.Client.ReplyIncludeServerTime = true
	cfg.RPC.Registry = reg
	var h *Handler
	h, _ = newRegistryTestHandler(t, cfg, registry)
	return h
}

// newDedupClient returns a client of userID, whose install instance is instance when not empty. Its
// token session is zero.
func newDedupClient(id string, userID string, ctx context.Context, instance string) *tools.TestClientMock {
	storage := map[string]any{}
	if instance != "" {
		storage[clientstorage.KeyInstance] = instance
	}
	client := newRegistryTestClient(id, userID, ctx, storage)
	client.IdentityFunc = func() centrifuge.TokenIdentity {
		return centrifuge.TokenIdentity{}
	}
	return client
}

// dedupMsgID returns a msg_id of the current second, with sequence seq.
func dedupMsgID(seq uint64) uint64 {
	return uint64(time.Now().Unix())<<rpcdedup.MsgIDSeqBits | seq
}

// dedupCall calls method with msgID and data through OnRPC.
func dedupCall(h *Handler, client *tools.TestClientMock, method string, msgID uint64, data string) (centrifuge.RPCReply, error) {
	return h.OnRPC(client, centrifuge.RPCEvent{
		Method: method,
		Data:   []byte(data),
		Call:   centrifuge.RPCCallOptions{MsgID: msgID},
	}, nil)
}

// dedupResult is the result of an asynchronous call.
type dedupResult struct {
	reply centrifuge.RPCReply
	err   error
}

// dedupCallAsync runs dedupCall in a goroutine, and returns the channel of its result.
func dedupCallAsync(h *Handler, client *tools.TestClientMock, method string, msgID uint64, data string) <-chan dedupResult {
	result := make(chan dedupResult, 1)
	go func() {
		reply, err := dedupCall(h, client, method, msgID, data)
		result <- dedupResult{reply: reply, err: err}
	}()
	return result
}

// waitDedupResult waits for an asynchronous call to finish.
func waitDedupResult(t testing.TB, result <-chan dedupResult) dedupResult {
	t.Helper()
	select {
	case r := <-result:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("call did not finish")
	}
	return dedupResult{}
}

// requireCancelledError checks that err is the cancelled error of the registry config: code 107, "bad request".
func requireCancelledError(t *testing.T, err error) {
	t.Helper()
	protoErr := requireProtocolError(t, err)
	require.Equal(t, uint32(107), protoErr.Code)
	require.Equal(t, "bad request", protoErr.Message)
}

func TestRPCDedupSameNodeDuplicateJoinsOneCall(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	first := newDedupClient("c1", "42", nil, testInstance)
	second := newDedupClient("c2", "42", nil, testInstance)
	msgID :=dedupMsgID(1)

	firstDone := dedupCallAsync(h, first, "users.get", msgID, "gate")
	backend.waitEntered(t)
	secondDone := dedupCallAsync(h, second, "users.get", msgID, "gate")
	// The duplicate joins the call in flight. Should it arrive after the release, it gets the record: either
	// way the backend runs once.
	time.Sleep(50 * time.Millisecond)
	backend.releaseGate()

	r1 := waitDedupResult(t, firstDone)
	r2 := waitDedupResult(t, secondDone)
	require.NoError(t, r1.err)
	require.NoError(t, r2.err)
	require.Equal(t, []byte("reply:gate"), r1.reply.Data)
	require.Equal(t, []byte("reply:gate"), r2.reply.Data)
	require.True(t, r2.reply.Cached)
	require.Len(t, backend.Calls(), 1)
}

func TestRPCDedupOwnerDisconnectKeepsMutation(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	var ownerCtx context.Context
	var disconnectOwner context.CancelFunc
	ownerCtx, disconnectOwner = context.WithCancel(context.Background())
	owner := newDedupClient("c1", "42", ownerCtx, testInstance)
	msgID :=dedupMsgID(2)

	ownerDone := dedupCallAsync(h, owner, "users.get", msgID, "gate")
	backend.waitEntered(t)
	disconnectOwner()
	backend.releaseGate()

	// The mutation was not cancelled with its connection: the backend committed and answered.
	ownerResult := waitDedupResult(t, ownerDone)
	require.NoError(t, ownerResult.err)
	require.Equal(t, []byte("reply:gate"), ownerResult.reply.Data)

	retransmit := newDedupClient("c2", "42", nil, testInstance)
	r := dedupCallAsyncResult(t, h, retransmit, "users.get", msgID, "gate")
	require.NoError(t, r.err)
	require.True(t, r.reply.Cached)
	require.Equal(t, []byte("reply:gate"), r.reply.Data)
	require.Len(t, backend.Calls(), 1)
}

// dedupCallAsyncResult runs a call and returns its result, failing the test when it does not finish.
func dedupCallAsyncResult(t *testing.T, h *Handler, client *tools.TestClientMock, method string, msgID uint64, data string) dedupResult {
	t.Helper()
	return waitDedupResult(t, dedupCallAsync(h, client, method, msgID, data))
}

func TestRPCDedupIdempotentCallCancelledOnDisconnect(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	var ctx context.Context
	var disconnect context.CancelFunc
	ctx, disconnect = context.WithCancel(context.Background())
	client := newDedupClient("c1", "42", ctx, testInstance)

	done := dedupCallAsync(h, client, "users.read", dedupMsgID(3), "gate")
	backend.waitEntered(t)
	disconnect()

	result := waitDedupResult(t, done)
	requireCancelledError(t, result.err)
}

func TestRPCDedupCancelThenRetransmit(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID :=dedupMsgID(4)

	done := dedupCallAsync(h, client, "users.get", msgID, "gate")
	backend.waitEntered(t)
	reply, err := h.OnRPC(client, centrifuge.RPCEvent{Call: centrifuge.RPCCallOptions{Cancel: msgID}}, nil)
	require.NoError(t, err)
	require.Empty(t, reply.Data)

	requireCancelledError(t, waitDedupResult(t, done).err)

	retransmit := dedupCallAsyncResult(t, h, client, "users.get", msgID, "gate")
	requireCancelledError(t, retransmit.err)
	require.True(t, retransmit.reply.Cached)
	require.Len(t, backend.Calls(), 1)
}

// A cancel which arrives before its call leaves a record, so the call never runs.
func TestRPCDedupCancelBeforeCallNeverRuns(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID :=dedupMsgID(5)

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Call: centrifuge.RPCCallOptions{Cancel: msgID}}, nil)
	require.NoError(t, err)
	result := dedupCallAsyncResult(t, h, client, "users.get", msgID, "x")
	requireCancelledError(t, result.err)
	require.True(t, result.reply.Cached)
	require.Empty(t, backend.Calls())
}

func TestRPCDedupMsgIDOutsideWindowIsInvalid(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)

	past := uint64(time.Now().Add(-200*time.Second).Unix())<<rpcdedup.MsgIDSeqBits | 1
	future := uint64(time.Now().Add(time.Hour).Unix())<<rpcdedup.MsgIDSeqBits | 1
	for _, msgID := range []uint64{past, future} {
		_, err := dedupCall(h, client, "users.get", msgID, "x")
		protoErr := requireProtocolError(t, err)
		require.Equal(t, uint32(107), protoErr.Code)
		require.Equal(t, "invalid msg_id", protoErr.Message)
	}
	require.Empty(t, backend.Calls())
}

// msg_id 0 means no dedup: each call runs.
func TestRPCDedupZeroMsgIDBypasses(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)

	for i := 0; i < 2; i++ {
		r := dedupCallAsyncResult(t, h, client, "users.get", 0, "x")
		require.NoError(t, r.err)
		require.False(t, r.reply.Cached)
	}
	require.Len(t, backend.Calls(), 2)
}

// A call without a scope runs each time, even with the same msg_id.
func TestRPCDedupNoScopeRunsEachTime(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, "")
	msgID :=dedupMsgID(6)

	for i := 0; i < 2; i++ {
		r := dedupCallAsyncResult(t, h, client, "users.get", msgID, "x")
		require.NoError(t, r.err)
		require.False(t, r.reply.Cached)
	}
	require.Len(t, backend.Calls(), 2)
}

// An ack forgets the record of the acknowledged call, so the msg_id runs again.
func TestRPCDedupAckRemovesRecord(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID :=dedupMsgID(7)

	first := dedupCallAsyncResult(t, h, client, "users.get", msgID, "x")
	require.NoError(t, first.err)

	_, err := h.OnRPC(client, centrifuge.RPCEvent{
		Method: "help.config",
		Data:   []byte("ack"),
		Call:   centrifuge.RPCCallOptions{Ack: []uint64{msgID}},
	}, nil)
	require.NoError(t, err)

	again := dedupCallAsyncResult(t, h, client, "users.get", msgID, "x")
	require.NoError(t, again.err)
	require.False(t, again.reply.Cached)
	require.Len(t, backend.Calls(), 3)
}

// A reused msg_id with other data is refused, and never gets the reply of the first call.
func TestRPCDedupReusedMsgIDWithOtherDataIsRefused(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID :=dedupMsgID(8)

	first := dedupCallAsyncResult(t, h, client, "users.get", msgID, "a")
	require.NoError(t, first.err)

	_, err := dedupCall(h, client, "users.get", msgID, "b")
	protoErr := requireProtocolError(t, err)
	require.Equal(t, "msg_id reused", protoErr.Message)

	// Another method with the same msg_id and data is refused too.
	_, err = dedupCall(h, client, "users.put", msgID, "a")
	require.Equal(t, "msg_id reused", requireProtocolError(t, err).Message)

	// The identical retransmit still gets the record.
	same := dedupCallAsyncResult(t, h, client, "users.get", msgID, "a")
	require.NoError(t, same.err)
	require.True(t, same.reply.Cached)
	require.Equal(t, []byte("reply:a"), same.reply.Data)
	require.Len(t, backend.Calls(), 1)
}

// A reused msg_id with other data does not join the call in flight.
func TestRPCDedupReusedMsgIDInFlightIsNotJoined(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID :=dedupMsgID(9)

	owner := dedupCallAsync(h, client, "users.get", msgID, "gate")
	backend.waitEntered(t)
	_, err := dedupCall(h, client, "users.get", msgID, "x")
	require.Equal(t, "msg_id reused", requireProtocolError(t, err).Message)
	require.Len(t, backend.Calls(), 1)

	backend.releaseGate()
	require.NoError(t, waitDedupResult(t, owner).err)
}

// The instance of one account is no scope for another: a replay with another user runs the call.
func TestRPCDedupInstanceOfAnotherAccountCannotReplay(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	msgID :=dedupMsgID(10)

	owner := newDedupClient("c1", "42", nil, testInstance)
	require.NoError(t, dedupCallAsyncResult(t, h, owner, "users.get", msgID, "x").err)

	other := newDedupClient("c2", "43", nil, testInstance)
	r := dedupCallAsyncResult(t, h, other, "users.get", msgID, "x")
	require.NoError(t, r.err)
	require.False(t, r.reply.Cached)
	require.Len(t, backend.Calls(), 2)
}

// failingDedupStore fails every Begin, as a store which lost its backend.
type failingDedupStore struct {
	*rpcdedup.Memory
}

func (s failingDedupStore) Begin(key rpcdedup.Key, flight *rpcdedup.Flight) (rpcdedup.Lookup, error) {
	return rpcdedup.Lookup{}, errors.New("store down")
}

func TestRPCDedupStoreFailureFailsClosed(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	h.rpcDedup = failingDedupStore{Memory: h.rpcDedup.(*rpcdedup.Memory)}
	client := newDedupClient("c1", "42", nil, testInstance)

	_, err := dedupCall(h, client, "users.get", dedupMsgID(11), "x")
	protoErr := requireProtocolError(t, err)
	require.Equal(t, uint32(108), protoErr.Code)
	require.True(t, protoErr.Temporary)
	require.Empty(t, backend.Calls())

	// An idempotent call does not use the store, and is not affected.
	r := dedupCallAsyncResult(t, h, client, "users.read", dedupMsgID(12), "x")
	require.NoError(t, r.err)
}

func TestRPCDedupStoreFailureFailsOpen(t *testing.T) {
	backend := newDedupBackend(t)
	reg := dedupRegistryConfig()
	reg.Dedup.OnStoreError = configtypes.RPCDedupFailOpen
	h := newDedupHandler(t, reg, backend)
	h.rpcDedup = failingDedupStore{Memory: h.rpcDedup.(*rpcdedup.Memory)}
	client := newDedupClient("c1", "42", nil, testInstance)

	r := dedupCallAsyncResult(t, h, client, "users.get", dedupMsgID(13), "x")
	require.NoError(t, r.err)
	require.False(t, r.reply.Cached)
	require.Len(t, backend.Calls(), 1)
}

// A cancel command is answered while the semaphore of the connection is full: it does not wait behind the calls
// it cancels. The connection is a real one, with two concurrent calls.
func TestRPCDedupCancelWhileSemaphoreIsFull(t *testing.T) {
	backend := newDedupBackend(t)
	reg := dedupRegistryConfig()
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	cfg := config.DefaultConfig()
	cfg.Client.Insecure = true
	cfg.Client.Concurrency = 2
	cfg.Client.ReplyIncludeServerTime = true
	cfg.RPC.Registry = reg
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, emptyJWTVerifier(t, cfgContainer), nil, &ProxyMap{})
	h.SetRPCRegistry(startDedupRegistry(t, reg, backend.conn(t)))
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)
	require.NoError(t, h.Setup())

	transport := newRecordingTransport()
	client, closeFn, err := centrifuge.NewClient(context.Background(), node, transport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn() })
	sendRateLimitCommand(client, &protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{}})
	require.Nil(t, transport.waitReply(t, 1).Error)

	first := dedupMsgID(20)
	second := dedupMsgID(21)
	// The client protocol is JSON, so the payload is a JSON string, which the backend matches as gate.
	sendRateLimitCommand(client, &protocol.Command{Id: 2, Rpc: &protocol.RPCRequest{Method: "help.config", Data: []byte(`"gate"`), MsgId: first}})
	backend.waitEntered(t)
	sendRateLimitCommand(client, &protocol.Command{Id: 3, Rpc: &protocol.RPCRequest{Method: "help.config", Data: []byte(`"gate"`), MsgId: second}})
	backend.waitEntered(t)

	// Both slots are taken by gated calls. The cancel must still be answered.
	sendRateLimitCommand(client, &protocol.Command{Id: 4, Rpc: &protocol.RPCRequest{Cancel: first}})
	cancelReply := transport.waitReply(t, 4)
	require.Nil(t, cancelReply.Error)

	cancelled := transport.waitReply(t, 2)
	require.NotNil(t, cancelled.Error)
	require.Equal(t, uint32(107), cancelled.Error.Code)

	backend.releaseGate()
	require.Nil(t, transport.waitReply(t, 3).Error)
}

// A retransmit and a call without updates reach the target as headers, and a plain call carries neither.
func TestRPCDedupRetransmitAndWithoutUpdatesHeaders(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID :=dedupMsgID(14)

	first := dedupCallAsyncResult(t, h, client, "users.get", msgID, "x")
	require.NoError(t, first.err)

	_, err := h.OnRPC(client, centrifuge.RPCEvent{
		Method: "users.get",
		Data:   []byte("x"),
		Call:   centrifuge.RPCCallOptions{MsgID: dedupMsgID(15), Retransmit: true, WithoutUpdates: true},
	}, nil)
	require.NoError(t, err)

	calls := backend.Calls()
	require.Len(t, calls, 2)
	require.Empty(t, calls[0].Metadata.Get("x-centrifugo-retransmit"))
	require.Empty(t, calls[0].Metadata.Get("x-centrifugo-without-updates"))
	require.Equal(t, []string{"1"}, calls[1].Metadata.Get("x-centrifugo-retransmit"))
	require.Equal(t, []string{"1"}, calls[1].Metadata.Get("x-centrifugo-without-updates"))
}
