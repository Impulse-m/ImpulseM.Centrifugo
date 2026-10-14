package tools

import (
	"github.com/centrifugal/centrifuge"
)

// RebindMockStubs are the client methods the rebind path of the client handler calls, with settable behaviour.
// Unset functions return the zero value, except Disconnect and Unsubscribe, which panic so that an unexpected call
// fails the test. It is embedded in TestClientMock.
type RebindMockStubs struct {
	ExtStateFunc    func() any
	DisconnectFunc  func(disconnect ...centrifuge.Disconnect)
	RebindFunc      func(opts centrifuge.RebindOptions) error
	SubscribeFunc   func(channel string, opts ...centrifuge.SubscribeOption) error
	ChannelsFunc    func() map[string]centrifuge.ChannelContext
	UnsubscribeFunc func(ch string, unsubscribe ...centrifuge.Unsubscribe)
}

func (m *RebindMockStubs) Disconnect(disconnect ...centrifuge.Disconnect) {
	if m.DisconnectFunc == nil {
		//TODO implement me
		panic("implement me")
	}
	m.DisconnectFunc(disconnect...)
}

func (m *RebindMockStubs) Unsubscribe(ch string, unsubscribe ...centrifuge.Unsubscribe) {
	if m.UnsubscribeFunc == nil {
		//TODO implement me
		panic("implement me")
	}
	m.UnsubscribeFunc(ch, unsubscribe...)
}

func (m *RebindMockStubs) ExtState() any {
	if m.ExtStateFunc != nil {
		return m.ExtStateFunc()
	}
	return nil
}

func (m *RebindMockStubs) Rebind(opts centrifuge.RebindOptions) error {
	if m.RebindFunc != nil {
		return m.RebindFunc(opts)
	}
	return nil
}

func (m *RebindMockStubs) Subscribe(channel string, opts ...centrifuge.SubscribeOption) error {
	if m.SubscribeFunc != nil {
		return m.SubscribeFunc(channel, opts...)
	}
	return nil
}

func (m *RebindMockStubs) ChannelsWithContext() map[string]centrifuge.ChannelContext {
	if m.ChannelsFunc != nil {
		return m.ChannelsFunc()
	}
	return nil
}
