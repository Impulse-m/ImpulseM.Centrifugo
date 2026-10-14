package tools

import (
	"github.com/centrifugal/centrifuge"
)

// RebindMockStubs are the client methods the rebind path of the client handler calls, with settable behaviour.
// Unset functions return the zero value.
type RebindMockStubs struct {
	ExtStateFunc  func() any
	RebindFunc    func(opts centrifuge.RebindOptions) error
	SubscribeFunc func(channel string, opts ...centrifuge.SubscribeOption) error
	ChannelsFunc  func() map[string]centrifuge.ChannelContext
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

// RebindClientMock is a TestClientMock whose Disconnect and Unsubscribe call the functions of this type.
// TestClientMock keeps its own methods of those names, which shadow the embedded stubs, so the methods are defined
// here. Disconnect and Unsubscribe panic when their function is not set, so that an unexpected call fails the test.
type RebindClientMock struct {
	TestClientMock
	DisconnectFunc  func(disconnect ...centrifuge.Disconnect)
	UnsubscribeFunc func(ch string, unsubscribe ...centrifuge.Unsubscribe)
}

func (m *RebindClientMock) Disconnect(disconnect ...centrifuge.Disconnect) {
	if m.DisconnectFunc == nil {
		panic("RebindClientMock: DisconnectFunc not set")
	}
	m.DisconnectFunc(disconnect...)
}

func (m *RebindClientMock) Unsubscribe(ch string, unsubscribe ...centrifuge.Unsubscribe) {
	if m.UnsubscribeFunc == nil {
		panic("RebindClientMock: UnsubscribeFunc not set")
	}
	m.UnsubscribeFunc(ch, unsubscribe...)
}
