package rpcdedup

import (
	"encoding/json"
	"errors"
)

// CancelNotificationOp is the op of the node notification which asks the node that runs a call to cancel it.
const CancelNotificationOp = "rpc_dedup_cancel"

// errMalformedCancel is returned by DecodeCancel for a payload which is not a cancel notification.
var errMalformedCancel = errors.New("malformed cancel notification")

// cancelNotification is the payload of CancelNotificationOp. Its fields are short, as the payload is sent per
// cancelled call.
type cancelNotification struct {
	Scope string `json:"s"`
	MsgID uint64 `json:"m"`
}

// EncodeCancel returns the payload of CancelNotificationOp for key.
func EncodeCancel(key Key) []byte {
	// Marshalling a struct of a string and an integer does not fail.
	data, _ := json.Marshal(cancelNotification(key))
	return data
}

// DecodeCancel returns the key of a payload of CancelNotificationOp. A payload without a scope is malformed.
func DecodeCancel(data []byte) (Key, error) {
	var n cancelNotification
	if err := json.Unmarshal(data, &n); err != nil {
		return Key{}, err
	}
	if n.Scope == "" {
		return Key{}, errMalformedCancel
	}
	return Key(n), nil
}
