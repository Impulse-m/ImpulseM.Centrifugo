package proxyproto

import "encoding/json"

func (e *JSONEncoder) EncodeNotifyChannelStateRequest(req *NotifyChannelStateRequest) ([]byte, error) {
	return json.Marshal(req)
}

func (e *JSONDecoder) DecodeNotifyChannelStateResponse(data []byte) (*NotifyChannelStateResponse, error) {
	var resp NotifyChannelStateResponse
	err := json.Unmarshal(data, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}
