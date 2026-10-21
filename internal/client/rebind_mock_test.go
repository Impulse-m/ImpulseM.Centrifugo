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
	ExtStateFunc func() any
	NotConnected bool
	// DisconnectHandler is the handler OnDisconnectIfConnected sets.
	DisconnectHandler centrifuge.DisconnectHandler
	RebindFunc        func(opts centrifuge.RebindOptions) error
	SubscribeFunc     func(channel string, opts ...centrifuge.SubscribeOption) error
	ChannelsFunc      func() map[string]centrifuge.ChannelContext
	UnsubscribeFunc   func(ch string, unsubscribe ...centrifuge.Unsubscribe)
}

func (m *rebindClientMock) ExtState() any {
	if m.ExtStateFunc == nil {
		return nil
	}
	return m.ExtStateFunc()
}

// SetExtState does nothing, as the mock keeps no extension state unless ExtStateFunc is set.
func (m *rebindClientMock) SetExtState(v any) {
}

// OnDisconnectIfConnected reports whether the handler was set: false when NotConnected is set, else true. The handler is
// kept in DisconnectHandler.
func (m *rebindClientMock) OnDisconnectIfConnected(h centrifuge.DisconnectHandler) bool {
	if m.NotConnected {
		return false
	}
	m.DisconnectHandler = h
	return true
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

func (m *rebindClientMock) BeginDisconnect(disconnect centrifuge.Disconnect) bool {
	panic("rebindClientMock: BeginDisconnect is not expected, a test closes a real connection")
}

func (m *rebindClientMock) Closing() bool {
	return false
}

func (m *rebindClientMock) Unsubscribe(ch string, unsubscribe ...centrifuge.Unsubscribe) {
	if m.UnsubscribeFunc == nil {
		panic("rebindClientMock: UnsubscribeFunc not set")
	}
	m.UnsubscribeFunc(ch, unsubscribe...)
}
