package unigrpc

import (
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/clientaddr"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/logging"
	"github.com/centrifugal/centrifugo/v6/internal/unigrpc/unistream"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

// RegisterService ...
func RegisterService(server *grpc.Server, service unistream.CentrifugoUniStreamServer) error {
	unistream.RegisterCentrifugoUniStreamServer(server, service)
	return nil
}

// Service can work with client GRPC connections.
type Service struct {
	unistream.UnimplementedCentrifugoUniStreamServer
	config        Config
	clientAddress configtypes.ClientAddress
	node          *centrifuge.Node
}

// NewService creates new Service. clientAddress decides whether the peer address of a stream is
// the client address of its connection.
func NewService(n *centrifuge.Node, c Config, clientAddress configtypes.ClientAddress) *Service {
	return &Service{
		config:        c,
		clientAddress: clientAddress,
		node:          n,
	}
}

// Consume is a unidirectional server->client stream with real-time data.
func (s *Service) Consume(req *unistream.ConnectRequest, stream unistream.CentrifugoUniStream_ConsumeServer) error {
	streamDataCh := make(chan rawFrame)
	transport := newGRPCTransport(stream, streamDataCh)

	connectRequest := &protocol.ConnectRequest{
		Token:   req.Token,
		Data:    req.Data,
		Name:    req.Name,
		Version: req.Version,
		Headers: req.Headers,
	}
	if req.Subs != nil {
		subs := make(map[string]*protocol.SubscribeRequest, len(req.Subs))
		for k, v := range req.Subs {
			subs[k] = &protocol.SubscribeRequest{
				Recover: v.Recover,
				Offset:  v.Offset,
				Epoch:   v.Epoch,
			}
		}
		connectRequest.Subs = subs
	}

	ctx := stream.Context()
	if s.clientAddress.Enabled {
		ctx = clientaddr.WithPeer(ctx)
	}
	c, closeFn, err := centrifuge.NewClient(ctx, s.node, transport)
	if err != nil {
		return err
	}
	defer func() { _ = closeFn() }()

	if logging.Enabled(logging.DebugLevel) {
		log.Debug().Str("transport", transport.Name()).Str("client", c.ID()).Msg("client connection established")
		defer func(started time.Time) {
			log.Debug().Str("transport", transport.Name()).Str("client", c.ID()).
				Str("duration", time.Since(started).String()).Msg("client connection completed")
		}(time.Now())
	}

	c.ProtocolConnect(connectRequest)

	for {
		select {
		case <-transport.closeCh:
			return nil
		case <-stream.Context().Done():
			return nil
		}
	}
}
