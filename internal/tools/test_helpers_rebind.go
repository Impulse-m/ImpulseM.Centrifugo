package tools

import (
	"github.com/centrifugal/centrifuge"
)

// RebindMockStubs are the client methods the rebind path of the client handler calls, with settable behaviour.
// Unset functions return the zero value, except Disconnect and Unsubscribe, which panic so that an unexpected call
// fails the test.
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
		panic("RebindMockStubs: DisconnectFunc not set")
	}
	m.DisconnectFunc(disconnect...)
}

func (m *RebindMockStubs) Unsubscribe(ch string, unsubscribe ...centrifuge.Unsubscribe) {
	if m.UnsubscribeFunc == nil {
		panic("RebindMockStubs: UnsubscribeFunc not set")
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

// RebindClientMock is a TestClientMock whose Disconnect and Unsubscribe call the rebind stubs. TestClientMock keeps
// its own methods of those names, which shadow the stubs, so they are forwarded here.
type RebindClientMock struct {
	TestClientMock
}

func (m *RebindClientMock) Disconnect(disconnect ...centrifuge.Disconnect) {
	m.TestClientMock.RebindMockStubs.Disconnect(disconnect...)
}

func (m *RebindClientMock) Unsubscribe(ch string, unsubscribe ...centrifuge.Unsubscribe) {
	m.TestClientMock.RebindMockStubs.Unsubscribe(ch, unsubscribe...)
}
