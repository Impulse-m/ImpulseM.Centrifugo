package client

import (
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
)

// rebindClientMock is a tools.TestClientMock with the methods of the rebind path: the extension state, the rebind,
// the subscriptions and the unsubscribe of a connection. Each method calls the function of the same name set in the
// mock. Unsubscribe and Disconnect panic, so that an unexpected call fails the test, and the methods without a function
// behave as a connection with nothing to change.
type rebindClientMock struct {
	tools.TestClientMock
	ExtStateFunc    func() any
	RebindFunc      func(opts centrifuge.RebindOptions) error
	SubscribeFunc   func(channel string, opts ...centrifuge.SubscribeOption) error
	ChannelsFunc    func() map[string]centrifuge.ChannelContext
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
	panic("rebindClientMock: Disconnect is not expected, a test closes a real connection")
}

func (m *rebindClientMock) Unsubscribe(ch string, unsubscribe ...centrifuge.Unsubscribe) {
	if m.UnsubscribeFunc == nil {
		panic("rebindClientMock: UnsubscribeFunc not set")
	}
	m.UnsubscribeFunc(ch, unsubscribe...)
}
