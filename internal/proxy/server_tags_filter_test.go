package proxy

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

const serverTagsFilterJSON = `{"op": "and", "nodes": [{"key": "type", "cmp": "eq", "val": "a"}, {"key": "tier", "cmp": "in", "vals": ["x", "y"]}]}`

func newServerTagsFilterProto() *proxyproto.FilterNode {
	return &proxyproto.FilterNode{
		Op: "and",
		Nodes: []*proxyproto.FilterNode{
			{Key: "type", Cmp: "eq", Val: "a"},
			{Key: "tier", Cmp: "in", Vals: []string{"x", "y"}},
		},
	}
}

func newServerTagsFilterProtocol() *protocol.FilterNode {
	return &protocol.FilterNode{
		Op: "and",
		Nodes: []*protocol.FilterNode{
			{Key: "type", Cmp: "eq", Val: "a"},
			{Key: "tier", Cmp: "in", Vals: []string{"x", "y"}},
		},
	}
}

func TestHandleSubscribeWithServerTagsFilter(t *testing.T) {
	chOpts := configtypes.ChannelOptions{
		SubscribeProxyEnabled: true,
		SubscribeProxyName:    "test",
	}
	opts := proxyGRPCTestServerOptions{
		ServerTagsFilter: newServerTagsFilterProto(),
	}

	grpcTestCase := newSubscribeHandleGRPCTestCase(context.Background(), newProxyGRPCTestServer("server tags filter", opts), chOpts)
	defer grpcTestCase.Teardown()

	httpTestCase := newSubscribeHandleHTTPTestCase(context.Background(), "/subscribe", chOpts)
	httpTestCase.Mux.HandleFunc("/subscribe", func(w http.ResponseWriter, req *http.Request) {
		_, _ = w.Write([]byte(fmt.Sprintf(`{"result": {"server_tags_filter": %s}}`, serverTagsFilterJSON)))
	})
	defer httpTestCase.Teardown()

	cases := newSubscribeHandleTestCases(httpTestCase, grpcTestCase)
	for _, c := range cases {
		reply, err := c.invokeHandle()
		require.NoError(t, err, c.protocol)
		require.Equal(t, newServerTagsFilterProtocol(), reply.Options.ServerTagsFilter, c.protocol)
	}
}

func TestHandleConnectWithSubscriptionServerTagsFilter(t *testing.T) {
	opts := proxyGRPCTestServerOptions{
		User:             "56",
		Channels:         []string{"test_ch"},
		ServerTagsFilter: newServerTagsFilterProto(),
	}
	grpcTestCase := newConnHandleGRPCTestCase(context.Background(), newProxyGRPCTestServer("subscription options", opts))
	defer grpcTestCase.Teardown()

	httpTestCase := newConnHandleHTTPTestCase(context.Background(), "/proxy")
	httpTestCase.Mux.HandleFunc("/proxy", func(w http.ResponseWriter, req *http.Request) {
		_, _ = w.Write([]byte(fmt.Sprintf(`{"result": {"user": "56", "subs": {"test_ch": {"server_tags_filter": %s}}}}`, serverTagsFilterJSON)))
	})
	defer httpTestCase.Teardown()

	cases := newConnHandleTestCases(httpTestCase, grpcTestCase)
	for _, c := range cases {
		reply, err := c.invokeHandle(context.Background())
		require.NoError(t, err, c.protocol)
		require.Contains(t, reply.Subscriptions, "test_ch", c.protocol)
		require.Equal(t, newServerTagsFilterProtocol(), reply.Subscriptions["test_ch"].ServerTagsFilter, c.protocol)
	}
}
