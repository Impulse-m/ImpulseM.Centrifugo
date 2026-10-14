package clientaddr

import (
	"context"

	"google.golang.org/grpc"
)

// StreamInterceptor is a gRPC stream interceptor which makes the peer address of a stream the
// client address of the stream, see WithPeer. It is for servers on PROXY protocol ports, where the
// peer is the PROXY source.
func StreamInterceptor(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return handler(srv, &peerStream{ServerStream: ss, ctx: WithPeer(ss.Context())})
}

// peerStream is a server stream whose context carries the client address.
type peerStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the context of the stream with the client address set.
func (s *peerStream) Context() context.Context {
	return s.ctx
}
