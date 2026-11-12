package apiproto

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// TestErrorPublishTagsLimitMapping checks the publish tags limit error keeps its code, message and statuses: HTTP 400 and
// gRPC InvalidArgument, like the other validation errors. It is not temporary: a retry of the same publish is refused too.
func TestErrorPublishTagsLimitMapping(t *testing.T) {
	require.Equal(t, uint32(1000), ErrorPublishTagsLimit.Code)
	require.Equal(t, "publish tags limit exceeded", ErrorPublishTagsLimit.Message)
	require.False(t, ErrorPublishTagsLimit.Temporary)
	require.Equal(t, http.StatusBadRequest, MapErrorToHTTPCode(ErrorPublishTagsLimit))
	require.Equal(t, codes.InvalidArgument, MapErrorToGRPCCode(ErrorPublishTagsLimit))
}

// TestDecodePublishServerTags checks the HTTP API decodes server_tags next to tags, and that a request without it has
// no server tags.
func TestDecodePublishServerTags(t *testing.T) {
	req, err := NewJSONRequestDecoder().DecodePublish([]byte(`{"channel":"chat:room1","data":{"text":"hi"},"tags":{"kind":"msg"},"server_tags":{"tenant":"t-17","member":"u-42"}}`))
	require.NoError(t, err)
	require.Equal(t, map[string]string{"kind": "msg"}, req.GetTags())
	require.Equal(t, map[string]string{"tenant": "t-17", "member": "u-42"}, req.GetServerTags())

	req, err = NewJSONRequestDecoder().DecodePublish([]byte(`{"channel":"chat:room1","data":{}}`))
	require.NoError(t, err)
	require.Nil(t, req.GetServerTags())
}

// TestDecodeBroadcastServerTags checks the HTTP API decodes server_tags of a broadcast, which applies to every channel.
func TestDecodeBroadcastServerTags(t *testing.T) {
	req, err := NewJSONRequestDecoder().DecodeBroadcast([]byte(`{"channels":["chat:a","chat:b"],"data":{},"server_tags":{"tenant":"t-17"}}`))
	require.NoError(t, err)
	require.Equal(t, []string{"chat:a", "chat:b"}, req.GetChannels())
	require.Equal(t, map[string]string{"tenant": "t-17"}, req.GetServerTags())
}

// TestPublishServerTagsProtobufRoundTrip checks the server_tags field survives the protobuf encoding of the gRPC API.
func TestPublishServerTagsProtobufRoundTrip(t *testing.T) {
	in := &PublishRequest{
		Channel:    "chat:room1",
		Data:       []byte(`{}`),
		Tags:       map[string]string{"kind": "msg"},
		ServerTags: map[string]string{"tenant": "t-17"},
	}
	raw, err := proto.Marshal(in)
	require.NoError(t, err)
	var out PublishRequest
	require.NoError(t, proto.Unmarshal(raw, &out))
	require.Equal(t, map[string]string{"tenant": "t-17"}, out.GetServerTags())
	require.Equal(t, map[string]string{"kind": "msg"}, out.GetTags())
}
