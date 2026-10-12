package proxy

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

type grpcSubRefreshHandleTestCase struct {
	*tools.CommonGRPCProxyTestCase
	subRefreshProxyHandler *SubRefreshHandler
	channelOpts            configtypes.ChannelOptions
}

func newSubRefreshHandleGRPCTestCase(ctx context.Context, proxyGRPCServer proxyGRPCTestServer, opts configtypes.ChannelOptions) grpcSubRefreshHandleTestCase {
	commonProxyTestCase := tools.NewCommonGRPCProxyTestCase(ctx, proxyGRPCServer)

	subRefreshProxy, err := NewGRPCSubRefreshProxy("default", getTestGrpcProxy(commonProxyTestCase))
	if err != nil {
		log.Fatalln("could not create grpc sub refresh proxy: ", err)
	}

	subRefreshProxyHandler := NewSubRefreshHandler(SubRefreshHandlerConfig{
		Proxies: map[string]SubRefreshProxy{
			"test": subRefreshProxy,
		},
	})

	return grpcSubRefreshHandleTestCase{commonProxyTestCase, subRefreshProxyHandler, opts}
}

type httpSubRefreshHandleTestCase struct {
	*tools.CommonHTTPProxyTestCase
	subRefreshProxyHandler *SubRefreshHandler
	channelOpts            configtypes.ChannelOptions
}

func newSubRefreshHandleHTTPTestCase(ctx context.Context, endpoint string, opts configtypes.ChannelOptions) httpSubRefreshHandleTestCase {
	commonProxyTestCase := tools.NewCommonHTTPProxyTestCase(ctx)

	subRefreshProxy, err := NewHTTPSubRefreshProxy(getTestHttpProxy(commonProxyTestCase, endpoint))
	if err != nil {
		log.Fatalln("could not create http sub refresh proxy: ", err)
	}

	subRefreshProxyHandler := NewSubRefreshHandler(SubRefreshHandlerConfig{
		Proxies: map[string]SubRefreshProxy{
			"test": subRefreshProxy,
		},
	})

	return httpSubRefreshHandleTestCase{commonProxyTestCase, subRefreshProxyHandler, opts}
}

type subRefreshHandleTestCase struct {
	subRefreshProxyHandler *SubRefreshHandler
	protocol               string
	node                   *centrifuge.Node
	client                 *centrifuge.Client
	channelOpts            configtypes.ChannelOptions
}

func (c subRefreshHandleTestCase) invokeHandle() (reply centrifuge.SubRefreshReply, err error) {
	subRefreshHandler := c.subRefreshProxyHandler.Handle()
	reply, _, err = subRefreshHandler(c.client, centrifuge.SubRefreshEvent{}, c.channelOpts, PerCallData{})

	return reply, err
}

func newSubRefreshHandleTestCases(httpTestCase httpSubRefreshHandleTestCase, grpcTestCase grpcSubRefreshHandleTestCase) []subRefreshHandleTestCase {
	return []subRefreshHandleTestCase{
		{
			subRefreshProxyHandler: grpcTestCase.subRefreshProxyHandler,
			node:                   grpcTestCase.Node,
			client:                 grpcTestCase.Client,
			channelOpts:            grpcTestCase.channelOpts,
			protocol:               "grpc",
		},
		{
			subRefreshProxyHandler: httpTestCase.subRefreshProxyHandler,
			node:                   httpTestCase.Node,
			client:                 httpTestCase.Client,
			channelOpts:            httpTestCase.channelOpts,
			protocol:               "http",
		},
	}
}

func TestHandleSubRefreshWithServerTagsFilter(t *testing.T) {
	chOpts := configtypes.ChannelOptions{
		SubRefreshProxyEnabled: true,
		SubRefreshProxyName:    "test",
	}
	opts := proxyGRPCTestServerOptions{
		ServerTagsFilter: newServerTagsFilterProto(),
	}

	grpcTestCase := newSubRefreshHandleGRPCTestCase(context.Background(), newProxyGRPCTestServer("server tags filter", opts), chOpts)
	defer grpcTestCase.Teardown()

	httpTestCase := newSubRefreshHandleHTTPTestCase(context.Background(), "/refresh", chOpts)
	httpTestCase.Mux.HandleFunc("/refresh", func(w http.ResponseWriter, req *http.Request) {
		_, _ = w.Write([]byte(fmt.Sprintf(`{"result": {"server_tags_filter": %s}}`, serverTagsFilterJSON)))
	})
	defer httpTestCase.Teardown()

	cases := newSubRefreshHandleTestCases(httpTestCase, grpcTestCase)
	for _, c := range cases {
		reply, err := c.invokeHandle()
		require.NoError(t, err, c.protocol)
		require.False(t, reply.Expired, c.protocol)
		require.Equal(t, newServerTagsFilterProtocol(), reply.ServerTagsFilter, c.protocol)
	}
}

func TestHandleSubRefreshWithoutServerTagsFilter(t *testing.T) {
	chOpts := configtypes.ChannelOptions{
		SubRefreshProxyEnabled: true,
		SubRefreshProxyName:    "test",
	}

	grpcTestCase := newSubRefreshHandleGRPCTestCase(context.Background(), newProxyGRPCTestServer("server tags filter", proxyGRPCTestServerOptions{}), chOpts)
	defer grpcTestCase.Teardown()

	httpTestCase := newSubRefreshHandleHTTPTestCase(context.Background(), "/refresh", chOpts)
	httpTestCase.Mux.HandleFunc("/refresh", func(w http.ResponseWriter, req *http.Request) {
		_, _ = w.Write([]byte(`{"result": {"expire_at": 0}}`))
	})
	defer httpTestCase.Teardown()

	cases := newSubRefreshHandleTestCases(httpTestCase, grpcTestCase)
	for _, c := range cases {
		reply, err := c.invokeHandle()
		require.NoError(t, err, c.protocol)
		require.Nil(t, reply.ServerTagsFilter, c.protocol)
	}
}
