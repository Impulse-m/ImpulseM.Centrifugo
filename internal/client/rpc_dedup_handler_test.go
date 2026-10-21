package client

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
	if string(in) == "fail" {
		return status.Error(codes.Unauthenticated, "denied")
	}
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
		configtypes.RPCMethod{Method: "users.anon", GRPCMethod: registryTestBackendMethod, Target: "backend", AuthClass: configtypes.RPCAuthClassAnonymous},
	)
	reg.Errors.MsgIDInvalid = configtypes.RPCRegistryError{Code: 107, Message: "invalid msg_id"}
	reg.Errors.MsgIDReused = configtypes.RPCRegistryError{Code: 107, Message: "msg_id reused"}
	reg.Errors.ResultEvicted = configtypes.RPCRegistryError{Code: 110, Message: "result expired"}
	reg.Errors.DedupUnavailable = configtypes.RPCRegistryError{Code: 108, Message: "dedup unavailable"}
	reg.Dedup = configtypes.RPCDedup{
		Enabled:            true,
		Window:             configtypes.Duration(120 * time.Second),
		MaxSkew:            configtypes.Duration(30 * time.Second),
		MaxReplySize:       65536,
		LocalMaxEntries:    1000,
		LocalMaxBytes:      1 << 28,
		ScopeReserve:       4,
		ScopeReserveBytes:  8192,
		MaxScopesPerUser:   8,
		MaxRecordsPerScope: 1024,
		OnStoreError:       configtypes.RPCDedupFailClosed,
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
func newDedupClient(id string, userID string, ctx context.Context, instance string) *rebindClientMock {
	storage := map[string]any{}
	if instance != "" {
		storage[rpcInstanceKey] = instance
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
func dedupCall(h *Handler, client *rebindClientMock, method string, msgID uint64, data string) (centrifuge.RPCReply, error) {
	return h.OnRPC(client, centrifuge.RPCEvent{
		Method: method,
		Data:   []byte(data),
		Call:   centrifuge.RPCCallOptions{MsgID: msgID},
	}, nil)
}

// testJoinSignal makes h signal each join of a duplicate to the returned channel, before the duplicate waits.
func testJoinSignal(h *Handler) <-chan struct{} {
	joined := make(chan struct{}, 8)
	h.rpcDedup.joined = func() {
		joined <- struct{}{}
	}
	return joined
}

// requireJoined waits until a duplicate joins a call in flight, as signalled by testJoinSignal.
func requireJoined(t *testing.T, joined <-chan struct{}) {
	t.Helper()
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("the duplicate did not join the call in flight")
	}
}

// dedupResult is the result of an asynchronous call.
type dedupResult struct {
	reply centrifuge.RPCReply
	err   error
}

// dedupCallAsync runs dedupCall in a goroutine, and returns the channel of its result.
func dedupCallAsync(h *Handler, client *rebindClientMock, method string, msgID uint64, data string) <-chan dedupResult {
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

// testDedupOutcomes replaces the dedup outcome counter of the metrics with a test vector, restored when the test
// ends. It must be called before the handler is made, which resolves its counters.
func testDedupOutcomes(t *testing.T) *prometheus.CounterVec {
	t.Helper()
	vec := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_rpc_registry_dedup_total"}, []string{"outcome"})
	previous := metrics.RPCRegistryDedupTotal
	metrics.RPCRegistryDedupTotal = vec
	t.Cleanup(func() {
		metrics.RPCRegistryDedupTotal = previous
	})
	return vec
}

// testDedupCount returns the count of an outcome of the test vector.
func testDedupCount(vec *prometheus.CounterVec, outcome string) float64 {
	return testutil.ToFloat64(vec.WithLabelValues(outcome))
}

func TestRPCDedupSameNodeDuplicateJoinsOneCall(t *testing.T) {
	vec := testDedupOutcomes(t)
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	first := newDedupClient("c1", "42", nil, testInstance)
	second := newDedupClient("c2", "42", nil, testInstance)
	msgID := dedupMsgID(1)

	firstDone := dedupCallAsync(h, first, "users.get", msgID, "gate")
	backend.waitEntered(t)
	joinSignal := testJoinSignal(h)
	secondDone := dedupCallAsync(h, second, "users.get", msgID, "gate")
	// The duplicate joins the call in flight: it is counted as a local join before the call is released.
	requireJoined(t, joinSignal)
	require.Equal(t, 1.0, testDedupCount(vec, "hit_inflight_local"))
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

// A duplicate whose connection closes while it waits frees its slot at once, and the call it joined goes on.
func TestRPCDedupClosedDuplicateFreesSlot(t *testing.T) {
	vec := testDedupOutcomes(t)
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	first := newDedupClient("c1", "42", nil, testInstance)
	var duplicateCtx context.Context
	var closeDuplicate context.CancelFunc
	duplicateCtx, closeDuplicate = context.WithCancel(context.Background())
	duplicate := newDedupClient("c2", "42", duplicateCtx, testInstance)
	msgID := dedupMsgID(17)

	owner := dedupCallAsync(h, first, "users.get", msgID, "gate")
	backend.waitEntered(t)
	joinSignal := testJoinSignal(h)
	joined := dedupCallAsync(h, duplicate, "users.get", msgID, "gate")
	requireJoined(t, joinSignal)
	require.Equal(t, 1.0, testDedupCount(vec, "hit_inflight_local"))

	closeDuplicate()
	requireCancelledError(t, waitDedupResult(t, joined).err)

	backend.releaseGate()
	require.NoError(t, waitDedupResult(t, owner).err)
	require.Len(t, backend.Calls(), 1)
}

// A cancel with a msg_id outside the window is refused, and records nothing.
func TestRPCDedupCancelOutsideWindowIsRefused(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	stale := uint64(time.Now().Add(-200*time.Second).Unix())<<rpcdedup.MsgIDSeqBits | 1

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Call: centrifuge.RPCCallOptions{Cancel: stale}}, nil)
	protoErr := requireProtocolError(t, err)
	require.Equal(t, uint32(107), protoErr.Code)
	require.Equal(t, "invalid msg_id", protoErr.Message)
	require.Equal(t, 0, testStoreEntries(h.rpcDedup.store))
}

// With the dedup off, a cancel command is an ordinary call: it is not answered before the lookup.
func TestRPCDedupOffCancelFallsThrough(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.RPC.Registry = registryTestRegistryConfig()
	registry := startRegistryTestRegistry(t, cfg.RPC.Registry, newRegistryTestBackend(t))
	h, _ := newRegistryTestHandler(t, cfg, registry)
	client := newRegistryTestClient("c1", "42", nil, nil)

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Call: centrifuge.RPCCallOptions{Cancel: 5}}, nil)
	require.Equal(t, uint32(104), requireProtocolError(t, err).Code)
}

// A non-idempotent call without a scope is cancelled by a disconnect, as an idempotent call is: nothing is
// recorded for it, so its commit gives no answer a retransmit could take.
func TestRPCDedupNoScopeMutationCancelledOnDisconnect(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	var ctx context.Context
	var disconnect context.CancelFunc
	ctx, disconnect = context.WithCancel(context.Background())
	client := newDedupClient("c1", "42", ctx, "")

	done := dedupCallAsync(h, client, "users.get", dedupMsgID(18), "gate")
	backend.waitEntered(t)
	disconnect()

	requireCancelledError(t, waitDedupResult(t, done).err)
}

func TestRPCDedupOwnerDisconnectKeepsMutation(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	var ownerCtx context.Context
	var disconnectOwner context.CancelFunc
	ownerCtx, disconnectOwner = context.WithCancel(context.Background())
	owner := newDedupClient("c1", "42", ownerCtx, testInstance)
	msgID := dedupMsgID(2)

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

// dedupCallAsyncResult runs a call and returns its result, failing the test when it does not finish.
func dedupCallAsyncResult(t *testing.T, h *Handler, client *rebindClientMock, method string, msgID uint64, data string) dedupResult {
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
	msgID := dedupMsgID(4)

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
	msgID := dedupMsgID(5)

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
	msgID := dedupMsgID(6)

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
	msgID := dedupMsgID(7)

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
	msgID := dedupMsgID(8)

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
	msgID := dedupMsgID(9)

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
	msgID := dedupMsgID(10)

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
	h.rpcDedup.store = failingDedupStore{Memory: h.rpcDedup.store.(*rpcdedup.Memory)}
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
	h.rpcDedup.store = failingDedupStore{Memory: h.rpcDedup.store.(*rpcdedup.Memory)}
	client := newDedupClient("c1", "42", nil, testInstance)

	r := dedupCallAsyncResult(t, h, client, "users.get", dedupMsgID(13), "x")
	require.NoError(t, r.err)
	require.False(t, r.reply.Cached)
	require.Len(t, backend.Calls(), 1)
}

// A retransmit on the same connection, while a call runs untracked under fail_open, is not run again: the store
// refuses it again, and the call of its msg_id is in the in-flight map of the connection.
func TestRPCDedupStoreFailureFailsOpenRetransmitIsNotRerun(t *testing.T) {
	backend := newDedupBackend(t)
	reg := dedupRegistryConfig()
	reg.Dedup.OnStoreError = configtypes.RPCDedupFailOpen
	h := newDedupHandler(t, reg, backend)
	h.rpcDedup.store = failingDedupStore{Memory: h.rpcDedup.store.(*rpcdedup.Memory)}
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID := dedupMsgID(14)

	firstDone := dedupCallAsync(h, client, "users.get", msgID, "gate")
	backend.waitEntered(t)
	retransmit := waitDedupResult(t, dedupCallAsync(h, client, "users.get", msgID, "gate"))
	protoErr := requireProtocolError(t, retransmit.err)
	require.Equal(t, uint32(108), protoErr.Code)
	require.True(t, protoErr.Temporary)
	require.Len(t, backend.Calls(), 1)

	backend.releaseGate()
	first := waitDedupResult(t, firstDone)
	require.NoError(t, first.err)
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

// A backend answer which the status mapping turns into a disconnect is final: the retransmit gets that answer as
// a protocol error, and the backend is not called again.
func TestRPCDedupDisconnectIsFinalAnswer(t *testing.T) {
	backend := newDedupBackend(t)
	reg := dedupRegistryConfig()
	reg.GRPCStatusToCodeTransforms = configtypes.RPCGRPCStatusToCodeTransforms{
		{Status: "UNAUTHENTICATED", ToDisconnect: configtypes.TransformDisconnect{Code: 3501, Reason: "sign in again"}},
	}
	h := newDedupHandler(t, reg, backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID := dedupMsgID(16)

	_, err := dedupCall(h, client, "users.put", msgID, "fail")
	var disconnect *centrifuge.Disconnect
	require.ErrorAs(t, err, &disconnect)
	require.Equal(t, uint32(3501), disconnect.Code)

	retransmit := dedupCallAsyncResult(t, h, client, "users.put", msgID, "fail")
	protoErr := requireProtocolError(t, retransmit.err)
	require.Equal(t, uint32(3501), protoErr.Code)
	require.Equal(t, "sign in again", protoErr.Message)
	require.True(t, retransmit.reply.Cached)
	require.Len(t, backend.Calls(), 1)
}

// A retransmit and a call without updates reach the target as headers, and a plain call carries neither.
func TestRPCDedupRetransmitAndWithoutUpdatesHeaders(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID := dedupMsgID(14)

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

// A reload which widens the window does not widen the msg_ids the handler accepts: the window is kept with the
// store, whose records last for the window at start.
func TestRPCDedupReloadDoesNotWidenWindow(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	cfg := h.cfgContainer.Config()
	cfg.RPC.Registry.Dedup.Window = configtypes.Duration(600 * time.Second)
	require.NoError(t, h.cfgContainer.Reload(cfg))
	client := newDedupClient("c1", "42", nil, testInstance)

	past := uint64(time.Now().Add(-200*time.Second).Unix())<<rpcdedup.MsgIDSeqBits | 1
	_, err := dedupCall(h, client, "users.get", past, "x")
	protoErr := requireProtocolError(t, err)
	require.Equal(t, uint32(107), protoErr.Code)
	require.Equal(t, "invalid msg_id", protoErr.Message)
	require.Empty(t, backend.Calls())
}

// A duplicate which joins a call on its own connection does not take the in-flight entry of the call: a cancel of
// the msg_id on the connection reaches the call, and both the owner and the duplicate are answered cancelled.
func TestRPCDedupCancelReachesOwnerOfJoinedCall(t *testing.T) {
	vec := testDedupOutcomes(t)
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID := dedupMsgID(6)

	owner := dedupCallAsync(h, client, "users.get", msgID, "gate")
	backend.waitEntered(t)
	joinSignal := testJoinSignal(h)
	joined := dedupCallAsync(h, client, "users.get", msgID, "gate")
	// The join is signalled before the duplicate waits on the call, and counted on the hit counter.
	requireJoined(t, joinSignal)
	require.Equal(t, 1.0, testDedupCount(vec, "hit_inflight_local"))

	reply, err := h.OnRPC(client, centrifuge.RPCEvent{Call: centrifuge.RPCCallOptions{Cancel: msgID}}, nil)
	require.NoError(t, err)
	require.Empty(t, reply.Data)
	requireCancelledError(t, waitDedupResult(t, owner).err)
	requireCancelledError(t, waitDedupResult(t, joined).err)
	require.Len(t, backend.Calls(), 1)
}

// requireRetrySignal asserts err is a protocol error whose metadata is the retry signal retry, and nothing else.
func requireRetrySignal(t *testing.T, err error, retry string) {
	t.Helper()
	protoErr := requireProtocolError(t, err)
	require.Equal(t, map[string]string{rpcRetryMetadataKey: retry}, protoErr.Metadata)
}

// requireNoRetrySignal asserts err is a protocol error which carries no retry signal.
func requireNoRetrySignal(t *testing.T, err error) {
	t.Helper()
	protoErr := requireProtocolError(t, err)
	require.NotContains(t, protoErr.Metadata, rpcRetryMetadataKey)
}

// The retry signal key is under the reserved prefix, which the header allow-lists refuse. A backend cannot set it: the
// runtime removes it from every error built from a backend trailer.
func TestRPCRetryMetadataKeyIsReserved(t *testing.T) {
	require.True(t, strings.HasPrefix(rpcRetryMetadataKey, configtypes.RPCRegistryReservedHeaderPrefix))
}

// A msg_id outside the window is refused with the new_msg_id signal, and nothing runs.
func TestRPCDedupMsgIDOutsideWindowCarriesNewMsgIDSignal(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	past := uint64(time.Now().Add(-200*time.Second).Unix())<<rpcdedup.MsgIDSeqBits | 1

	_, err := dedupCall(h, client, "users.get", past, "x")
	require.False(t, requireProtocolError(t, err).Temporary)
	requireRetrySignal(t, err, rpcRetryNewMsgID)
	require.Empty(t, backend.Calls())
}

// A retransmit whose msg_id is outside the window is refused without the signal: its call may have run, so the client
// must not send it again with a new msg_id. Nothing runs.
func TestRPCDedupRetransmitOutsideWindowCarriesNoSignal(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	past := uint64(time.Now().Add(-200*time.Second).Unix())<<rpcdedup.MsgIDSeqBits | 1

	_, err := h.OnRPC(client, centrifuge.RPCEvent{
		Method: "users.put",
		Data:   []byte("x"),
		Call:   centrifuge.RPCCallOptions{MsgID: past, Retransmit: true},
	}, nil)
	require.Equal(t, "invalid msg_id", requireProtocolError(t, err).Message)
	requireNoRetrySignal(t, err)
	require.Empty(t, backend.Calls())
}

// A msg_id above 53 bits is refused with the new_msg_id signal, though its time part is in the window, and nothing runs.
func TestRPCDedupMsgIDAbove53BitsCarriesNewMsgIDSignal(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID := dedupMsgID(26) | 1<<53

	_, err := dedupCall(h, client, "users.get", msgID, "x")
	requireRetrySignal(t, err, rpcRetryNewMsgID)
	require.Empty(t, backend.Calls())
}

// A call refused by the store under fail_closed, for the user limit of scopes, is temporary and carries the later
// signal, and does not run.
func TestRPCDedupRefusedUserCarriesLaterSignal(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	testSmallDedupStore(h, 1000, 1)
	testFillSmallStore(t, h, backend, "4")

	refused := dedupCallAsyncResult(t, h, newDedupClient("c4b", "4", nil, otherTestInstance), "users.get", dedupMsgID(2), "x")
	require.True(t, requireProtocolError(t, refused.err).Temporary)
	requireRetrySignal(t, refused.err, rpcRetryLater)
	require.Len(t, backend.Calls(), 1)
}

// A call which fails at Begin under fail_closed is temporary and carries the later signal, and does not run.
func TestRPCDedupStoreFailureFailsClosedCarriesLaterSignal(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	h.rpcDedup.store = failingDedupStore{Memory: h.rpcDedup.store.(*rpcdedup.Memory)}
	client := newDedupClient("c1", "42", nil, testInstance)

	_, err := dedupCall(h, client, "users.get", dedupMsgID(11), "x")
	require.True(t, requireProtocolError(t, err).Temporary)
	requireRetrySignal(t, err, rpcRetryLater)
	require.Empty(t, backend.Calls())
}

// A duplicate which waits past the method timeout for the call it joined is answered with the later signal. The
// flight is never completed, so the wait ends at the timeout of the method.
func TestRPCDedupJoinTimeoutCarriesLaterSignal(t *testing.T) {
	reg := dedupRegistryConfig()
	reg.Methods = append(reg.Methods, configtypes.RPCMethod{
		Method:     "users.slow",
		GRPCMethod: registryTestBackendMethod,
		Target:     "backend",
		Timeout:    configtypes.Duration(50 * time.Millisecond),
	})
	h := newDedupHandler(t, reg, newDedupBackend(t))
	m := h.rpcRegistry.Lookup("users.slow")
	require.NotNil(t, m)
	key := rpcdedup.Key{Scope: rpcdedup.ScopeOf("i", "42", []byte(testInstance)), MsgID: dedupMsgID(22)}
	flight := rpcdedup.NewFlightWithFingerprint(rpcdedup.Fingerprint("users.slow", []byte("x")))
	lookup, err := h.rpcDedup.store.Begin(key, flight)
	require.NoError(t, err)
	require.Equal(t, rpcdedup.OutcomeMiss, lookup.Outcome)

	duplicate := newDedupClient("c2", "42", nil, testInstance)
	_, err = h.rpcJoin(duplicate, m, h.registryConfig(), flight)
	require.True(t, requireProtocolError(t, err).Temporary)
	requireRetrySignal(t, err, rpcRetryLater)
}

// A duplicate of a flight which was abandoned by its owner is answered without the signal: the call may have run.
func TestRPCDedupAbandonedOwnerCarriesNoSignal(t *testing.T) {
	h := newDedupHandler(t, dedupRegistryConfig(), newDedupBackend(t))
	m := h.rpcRegistry.Lookup("users.put")
	require.NotNil(t, m)
	key := rpcdedup.Key{Scope: rpcdedup.ScopeOf("i", "42", []byte(testInstance)), MsgID: dedupMsgID(23)}
	flight := rpcdedup.NewFlightWithFingerprint(rpcdedup.Fingerprint("users.put", []byte("x")))
	lookup, err := h.rpcDedup.store.Begin(key, flight)
	require.NoError(t, err)
	require.Equal(t, rpcdedup.OutcomeMiss, lookup.Outcome)
	h.completeRPCRecord(key, flight, rpcdedup.Record{Abandoned: true})

	duplicate := newDedupClient("c2", "42", nil, testInstance)
	_, err = h.rpcJoin(duplicate, m, h.registryConfig(), flight)
	require.True(t, requireProtocolError(t, err).Temporary)
	requireNoRetrySignal(t, err)
}

// A panic after the backend committed keeps a dedup_unavailable record, which the duplicate that joined it gets
// without the signal: the call ran, so a resend with the same msg_id must not be sent by the client. The backend runs
// once.
func TestRPCDedupPanicAfterBackendCarriesNoSignal(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	joined := testJoinSignal(h)
	h.rpcDedup.afterBackend = func() {
		panic("reply mapping failed")
	}
	client := newDedupClient("c1", "42", nil, testInstance)
	duplicate := newDedupClient("c2", "42", nil, testInstance)
	msgID := dedupMsgID(29)

	ownerPanic := make(chan any, 1)
	go func() {
		defer func() {
			ownerPanic <- recover()
		}()
		_, _ = dedupCall(h, client, "users.put", msgID, "gate")
	}()
	backend.waitEntered(t)
	duplicateDone := dedupCallAsync(h, duplicate, "users.put", msgID, "gate")
	requireJoined(t, joined)
	backend.releaseGate()

	require.NotNil(t, <-ownerPanic)
	dup := waitDedupResult(t, duplicateDone)
	require.True(t, requireProtocolError(t, dup.err).Temporary)
	requireNoRetrySignal(t, dup.err)
	require.Len(t, backend.Calls(), 1)
}

// A retransmit of a call whose owner panicked after the backend committed gets the error which the panic left in the
// record, without the signal, and the backend is not called again.
func TestRPCDedupPanicAfterBackendRetransmitIsNotRerun(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	h.rpcDedup.afterBackend = func() {
		panic("reply mapping failed")
	}
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID := dedupMsgID(30)

	ownerPanic := make(chan any, 1)
	go func() {
		defer func() {
			ownerPanic <- recover()
		}()
		_, _ = dedupCall(h, client, "users.put", msgID, "gate")
	}()
	backend.waitEntered(t)
	backend.releaseGate()
	require.NotNil(t, <-ownerPanic)

	reply, err := h.OnRPC(client, centrifuge.RPCEvent{
		Method: "users.put",
		Data:   []byte("gate"),
		Call:   centrifuge.RPCCallOptions{MsgID: msgID, Retransmit: true},
	}, nil)
	require.True(t, reply.Cached)
	require.True(t, requireProtocolError(t, err).Temporary)
	requireNoRetrySignal(t, err)
	require.Len(t, backend.Calls(), 1)
}

// A panic before the backend call keeps a dedup_unavailable record without the signal, as a panic after it does. The
// owner's client fails while the call is built, so the backend never runs. A retransmit gets the error from the record
// and the backend is not called.
func TestRPCDedupPanicBeforeBackendCarriesNoSignal(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	client.IDFunc = func() string {
		panic("client id failed")
	}
	msgID := dedupMsgID(31)

	ownerPanic := make(chan any, 1)
	go func() {
		defer func() {
			ownerPanic <- recover()
		}()
		_, _ = dedupCall(h, client, "users.put", msgID, "gate")
	}()
	require.NotNil(t, <-ownerPanic)
	require.Empty(t, backend.Calls())

	reply, err := h.OnRPC(client, centrifuge.RPCEvent{
		Method: "users.put",
		Data:   []byte("gate"),
		Call:   centrifuge.RPCCallOptions{MsgID: msgID, Retransmit: true},
	}, nil)
	require.True(t, reply.Cached)
	require.True(t, requireProtocolError(t, err).Temporary)
	requireNoRetrySignal(t, err)
	require.Empty(t, backend.Calls())
}

// A retransmit on the same connection, while a call runs untracked under fail_open, is refused without the signal:
// the call is running, and a resend would run it again.
func TestRPCDedupStoreFailureFailsOpenRetransmitCarriesNoSignal(t *testing.T) {
	backend := newDedupBackend(t)
	reg := dedupRegistryConfig()
	reg.Dedup.OnStoreError = configtypes.RPCDedupFailOpen
	h := newDedupHandler(t, reg, backend)
	h.rpcDedup.store = failingDedupStore{Memory: h.rpcDedup.store.(*rpcdedup.Memory)}
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID := dedupMsgID(24)

	firstDone := dedupCallAsync(h, client, "users.get", msgID, "gate")
	backend.waitEntered(t)
	retransmit := waitDedupResult(t, dedupCallAsync(h, client, "users.get", msgID, "gate"))
	require.True(t, requireProtocolError(t, retransmit.err).Temporary)
	requireNoRetrySignal(t, retransmit.err)

	backend.releaseGate()
	require.NoError(t, waitDedupResult(t, firstDone).err)
	require.Len(t, backend.Calls(), 1)
}

// A msg_id reused with other data is refused without the signal.
func TestRPCDedupReusedMsgIDCarriesNoSignal(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID := dedupMsgID(25)

	require.NoError(t, dedupCallAsyncResult(t, h, client, "users.get", msgID, "a").err)
	_, err := dedupCall(h, client, "users.get", msgID, "b")
	require.Equal(t, "msg_id reused", requireProtocolError(t, err).Message)
	requireNoRetrySignal(t, err)
}

// A reply over the dedup size limit is evicted, and its retransmit is refused without the signal: the call ran.
func TestRPCDedupResultEvictedCarriesNoSignal(t *testing.T) {
	backend := newDedupBackend(t)
	reg := dedupRegistryConfig()
	reg.Methods = append(reg.Methods, configtypes.RPCMethod{
		Method:           "users.big",
		GRPCMethod:       registryTestBackendMethod,
		Target:           "backend",
		MaxRequestBytes:  1 << 17,
		MaxResponseBytes: 1 << 17,
	})
	h := newDedupHandler(t, reg, backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID := dedupMsgID(26)
	big := strings.Repeat("x", 70000)

	first := dedupCallAsyncResult(t, h, client, "users.big", msgID, big)
	require.NoError(t, first.err)
	retransmit := dedupCallAsyncResult(t, h, client, "users.big", msgID, big)
	require.Equal(t, uint32(110), requireProtocolError(t, retransmit.err).Code)
	requireNoRetrySignal(t, retransmit.err)
	require.Len(t, backend.Calls(), 1)
}

// A cancel of a msg_id outside the window is refused without the signal: a cancel is not resent.
func TestRPCDedupCancelOutsideWindowCarriesNoSignal(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	stale := uint64(time.Now().Add(-200*time.Second).Unix())<<rpcdedup.MsgIDSeqBits | 1

	_, err := h.OnRPC(client, centrifuge.RPCEvent{Call: centrifuge.RPCCallOptions{Cancel: stale}}, nil)
	require.Equal(t, "invalid msg_id", requireProtocolError(t, err).Message)
	requireNoRetrySignal(t, err)
}

// A call which its cancel ends is answered cancelled, without the signal.
func TestRPCDedupCancelledCallCarriesNoSignal(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID := dedupMsgID(27)

	done := dedupCallAsync(h, client, "users.get", msgID, "gate")
	backend.waitEntered(t)
	_, err := h.OnRPC(client, centrifuge.RPCEvent{Call: centrifuge.RPCCallOptions{Cancel: msgID}}, nil)
	require.NoError(t, err)

	cancelled := waitDedupResult(t, done)
	requireCancelledError(t, cancelled.err)
	requireNoRetrySignal(t, cancelled.err)
}

// An error recorded for a call is replayed to its retransmit without the signal: the record holds the error the
// backend gave, and the call ran once.
func TestRPCDedupRecordedErrorReplayCarriesNoSignal(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID := dedupMsgID(28)

	first := dedupCallAsyncResult(t, h, client, "users.put", msgID, "fail")
	firstErr := requireProtocolError(t, first.err)
	retransmit := dedupCallAsyncResult(t, h, client, "users.put", msgID, "fail")
	require.True(t, retransmit.reply.Cached)
	require.Equal(t, firstErr.Code, requireProtocolError(t, retransmit.err).Code)
	requireNoRetrySignal(t, retransmit.err)
	require.Len(t, backend.Calls(), 1)
}

// A record written before the retry signal existed may hold it. Its replay to a retransmit drops the signal, since the
// call ran and the client must not send it again. The stored error keeps the signal: only the replay copies it.
func TestRPCDedupLegacyRecordReplayDropsRetrySignal(t *testing.T) {
	backend := newDedupBackend(t)
	reg := dedupRegistryConfig()
	h := newDedupHandler(t, reg, backend)
	msgID := dedupMsgID(47)
	key := rpcdedup.Key{Scope: rpcdedup.ScopeOf("i", "42", []byte(testInstance)), MsgID: msgID}
	flight := rpcdedup.NewFlightWithFingerprint(rpcdedup.Fingerprint("users.put", []byte("x")))
	lookup, err := h.rpcDedup.store.Begin(key, flight)
	require.NoError(t, err)
	require.Equal(t, rpcdedup.OutcomeMiss, lookup.Outcome)
	legacy := &centrifuge.Error{
		Code:      reg.Errors.DedupUnavailable.Code,
		Message:   reg.Errors.DedupUnavailable.Message,
		Temporary: true,
		Metadata:  map[string]string{rpcRetryMetadataKey: rpcRetryLater},
	}
	require.NoError(t, h.rpcDedup.store.Complete(key, flight, rpcdedup.Record{Err: legacy}))

	retransmit := dedupCallAsyncResult(t, h, newDedupClient("c1", "42", nil, testInstance), "users.put", msgID, "x")
	require.True(t, retransmit.reply.Cached)
	require.Equal(t, reg.Errors.DedupUnavailable.Code, requireProtocolError(t, retransmit.err).Code)
	requireNoRetrySignal(t, retransmit.err)
	require.Empty(t, backend.Calls())
	require.Equal(t, rpcRetryLater, legacy.Metadata[rpcRetryMetadataKey])
}
