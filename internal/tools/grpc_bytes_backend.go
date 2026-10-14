package tools

import (
	"context"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

// Payloads which make GRPCBytesBackend slow or answer with a large reply.
const (
	GRPCBytesSlowPayload = "slow"
	GRPCBytesBigPayload  = "big"
	GRPCBytesBigReply    = 1024
)

// GRPCBytesCall is one call received by GRPCBytesBackend.
type GRPCBytesCall struct {
	// Method is the full gRPC method name of the call.
	Method string
	// Metadata is the incoming metadata of the call.
	Metadata metadata.MD
}

// GRPCBytesFailure is the answer of GRPCBytesBackend to a method which is set to fail: the error
// returned by the call and the trailers sent with it.
type GRPCBytesFailure struct {
	// Err is the error of the call, usually a gRPC status error.
	Err error
	// Trailers are sent with the error.
	Trailers metadata.MD
}

// GRPCBytesBackend is an in-process gRPC server for tests. It answers every method with the raw
// request bytes, records the method and metadata of each call, and makes the call slow or large
// for the payloads above. A method set with FailMethod answers with its failure instead.
type GRPCBytesBackend struct {
	listener *bufconn.Listener
	server   *grpc.Server

	mu       sync.Mutex
	calls    []GRPCBytesCall
	conns    []*grpc.ClientConn
	failures map[string]GRPCBytesFailure
}

// NewGRPCBytesBackend starts a backend. Call Close when the test ends.
func NewGRPCBytesBackend() *GRPCBytesBackend {
	b := &GRPCBytesBackend{listener: bufconn.Listen(1 << 20)}
	b.server = grpc.NewServer(
		grpc.ForceServerCodec(GRPCBytesCodec{}),
		grpc.UnknownServiceHandler(b.handle),
	)
	go func() {
		if err := b.server.Serve(b.listener); err != nil && err != grpc.ErrServerStopped {
			log.Fatalf("GRPC bytes backend exited with error: %v", err)
		}
	}()
	return b
}

func (b *GRPCBytesBackend) handle(_ any, stream grpc.ServerStream) error {
	method, _ := grpc.MethodFromServerStream(stream)
	md, _ := metadata.FromIncomingContext(stream.Context())
	var in []byte
	err := stream.RecvMsg(&in)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.calls = append(b.calls, GRPCBytesCall{Method: method, Metadata: md})
	failure, failing := b.failures[method]
	b.mu.Unlock()

	if failing {
		if len(failure.Trailers) > 0 {
			stream.SetTrailer(failure.Trailers)
		}
		return failure.Err
	}
	if string(in) == GRPCBytesSlowPayload {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	out := in
	if string(in) == GRPCBytesBigPayload {
		out = make([]byte, GRPCBytesBigReply)
	}
	return stream.SendMsg(&out)
}

// Calls returns the calls received so far, in order.
func (b *GRPCBytesBackend) Calls() []GRPCBytesCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]GRPCBytesCall(nil), b.calls...)
}

// FailMethod makes calls of the full gRPC method name answer with failure. Calls of other methods
// are answered as before.
func (b *GRPCBytesBackend) FailMethod(method string, failure GRPCBytesFailure) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures == nil {
		b.failures = map[string]GRPCBytesFailure{}
	}
	b.failures[method] = failure
}

// Conn returns a client connection to the backend. It is closed by Close.
func (b *GRPCBytesBackend) Conn() (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(
		"passthrough:///grpc-bytes-backend",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return b.listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.conns = append(b.conns, conn)
	b.mu.Unlock()
	return conn, nil
}

// Close stops the server and closes the connections made by Conn.
func (b *GRPCBytesBackend) Close() {
	b.mu.Lock()
	conns := b.conns
	b.conns = nil
	b.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	b.server.Stop()
}

// GRPCBytesCodec is a gRPC codec which passes message bytes through, for a server or client which
// forwards raw payloads.
type GRPCBytesCodec struct{}

// Name returns the codec name, proto, as the registry targets use.
func (GRPCBytesCodec) Name() string {
	return "proto"
}

// Marshal returns the bytes of v, which is a []byte or a *[]byte.
func (GRPCBytesCodec) Marshal(v any) ([]byte, error) {
	if raw, ok := v.(*[]byte); ok {
		return *raw, nil
	}
	return v.([]byte), nil
}

// Unmarshal copies data into v, which must be a *[]byte.
func (GRPCBytesCodec) Unmarshal(data []byte, v any) error {
	raw := v.(*[]byte)
	*raw = append((*raw)[:0], data...)
	return nil
}
