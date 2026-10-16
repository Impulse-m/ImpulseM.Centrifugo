package rpcdedup

import (
	"encoding/json"
	"errors"
)

// CancelNotificationOp is the op of the node notification which asks the node that runs a call to cancel it.
const CancelNotificationOp = "rpc_dedup_cancel"

// DoneNotificationOp is the op of the node notification which tells a node that waits for a call that its record is
// written. The payload is the key, as for CancelNotificationOp.
const DoneNotificationOp = "rpc_dedup_done"

// errMalformedNotification is returned by the decoders for a payload which is not a key notification.
var errMalformedNotification = errors.New("malformed key notification")

// keyNotification is the payload of the key notifications. Its fields are short, as the payload is sent per call.
type keyNotification struct {
	Scope string `json:"s"`
	MsgID uint64 `json:"m"`
}

// EncodeCancel returns the payload of CancelNotificationOp for key.
func EncodeCancel(key Key) []byte {
	return encodeKey(key)
}

// DecodeCancel returns the key of a payload of CancelNotificationOp. A payload without a scope is malformed.
func DecodeCancel(data []byte) (Key, error) {
	return decodeKey(data)
}

// EncodeDone returns the payload of DoneNotificationOp for key.
func EncodeDone(key Key) []byte {
	return encodeKey(key)
}

// DecodeDone returns the key of a payload of DoneNotificationOp. A payload without a scope is malformed.
func DecodeDone(data []byte) (Key, error) {
	return decodeKey(data)
}

// encodeKey returns the payload of a key notification.
func encodeKey(key Key) []byte {
	// Marshalling a struct of a string and an integer does not fail.
	data, _ := json.Marshal(keyNotification(key))
	return data
}

// decodeKey returns the key of a payload of a key notification. A payload without a scope is malformed.
func decodeKey(data []byte) (Key, error) {
	var n keyNotification
	if err := json.Unmarshal(data, &n); err != nil {
		return Key{}, err
	}
	if n.Scope == "" {
		return Key{}, errMalformedNotification
	}
	return Key(n), nil
}
