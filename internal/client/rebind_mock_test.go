package client

import (
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
)

// rebindClientMock is a tools.TestClientMock with the methods of the rebind path: the extension state, the rebind,
// the subscriptions and the disconnect and unsubscribe of a connection. Each method calls the function of the same
// name set in the mock. Disconnect, Unsubscribe and the methods without a function panic when it is not set, so
// that an unexpected call fails the test.
type rebindClientMock struct {
	tools.TestClientMock
	ExtStateFunc    func() any
	RebindFunc      func(opts centrifuge.RebindOptions) error
	SubscribeFunc   func(channel string, opts ...centrifuge.SubscribeOption) error
	ChannelsFunc    func() map[string]centrifuge.ChannelContext
	DisconnectFunc  func(disconnect ...centrifuge.Disconnect)
	UnsubscribeFunc func(ch string, unsubscribe ...centrifuge.Unsubscribe)
}

func (m *rebindClientMock) ExtState() any {
	if m.ExtStateFunc == nil {
		return nil
	}
	return m.ExtStateFunc()
}

func (m *rebindClientMock) Rebind(opts centrifuge.RebindOptions) error {
	if m.RebindFunc == nil {
		return nil
	}
	return m.RebindFunc(opts)
}

func (m *rebindClientMock) Subscribe(channel string, opts ...centrifuge.SubscribeOption) error {
	if m.SubscribeFunc == nil {
		return nil
	}
	return m.SubscribeFunc(channel, opts...)
}

func (m *rebindClientMock) ChannelsWithContext() map[string]centrifuge.ChannelContext {
	if m.ChannelsFunc == nil {
		return nil
	}
	return m.ChannelsFunc()
}

func (m *rebindClientMock) Disconnect(disconnect ...centrifuge.Disconnect) {
	if m.DisconnectFunc == nil {
		panic("rebindClientMock: DisconnectFunc not set")
	}
	m.DisconnectFunc(disconnect...)
}

func (m *rebindClientMock) Unsubscribe(ch string, unsubscribe ...centrifuge.Unsubscribe) {
	if m.UnsubscribeFunc == nil {
		panic("rebindClientMock: UnsubscribeFunc not set")
	}
	m.UnsubscribeFunc(ch, unsubscribe...)
}
