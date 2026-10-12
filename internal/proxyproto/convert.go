package proxyproto

import (
	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
)

func DisconnectFromProto(s *Disconnect) *centrifuge.Disconnect {
	return &centrifuge.Disconnect{
		Code:   s.Code,
		Reason: s.Reason,
	}
}

func ErrorFromProto(s *Error) *centrifuge.Error {
	return &centrifuge.Error{
		Code:      s.Code,
		Message:   s.Message,
		Temporary: s.Temporary,
	}
}

func FilterNodeFromProto(s *FilterNode) *protocol.FilterNode {
	if s == nil {
		return nil
	}
	var nodes []*protocol.FilterNode
	for _, node := range s.Nodes {
		nodes = append(nodes, FilterNodeFromProto(node))
	}
	return &protocol.FilterNode{
		Op:    s.Op,
		Key:   s.Key,
		Cmp:   s.Cmp,
		Val:   s.Val,
		Vals:  s.Vals,
		Nodes: nodes,
	}
}
