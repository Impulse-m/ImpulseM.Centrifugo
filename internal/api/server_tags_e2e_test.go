package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
)

// serverTagsHTTPServer serves the API routes over the memory node of serverTagsTestSetup, under the /api prefix as the
// production handler does, and returns the server with the executor behind it.
func serverTagsHTTPServer(t *testing.T) (*httptest.Server, *Executor) {
	t.Helper()
	node, api := serverTagsTestSetup(t)
	handler := NewHandler(node, api, Config{})
	mux := http.NewServeMux()
	for path, route := range handler.Routes() {
		mux.Handle("/api"+path, route)
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server, api
}

// postServerTagsRequest posts a JSON body to an API path and returns the HTTP status code.
func postServerTagsRequest(t *testing.T, server *httptest.Server, path string, body string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, server.URL+"/api"+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

// TestHTTPPublishServerTagsStoredWithoutTheirPublicTags checks a publish over HTTP with server tags is accepted, and the
// stored publication keeps its public tags while the server API history does not return the server tags.
func TestHTTPPublishServerTagsStoredWithoutTheirPublicTags(t *testing.T) {
	server, api := serverTagsHTTPServer(t)

	status := postServerTagsRequest(t, server, "/publish", `{"channel":"st:room","data":{"text":"hi"},"tags":{"kind":"msg"},"server_tags":{"tenant":"t-17"}}`)
	require.Equal(t, http.StatusOK, status)

	history := api.History(context.Background(), &HistoryRequest{Channel: "st:room", Limit: -1})
	require.Nil(t, history.Error)
	require.Len(t, history.Result.Publications, 1)
	require.Equal(t, map[string]string{"kind": "msg"}, history.Result.Publications[0].Tags)
}

// TestHTTPPublishServerTagsNotAllowedRefused checks a publish over HTTP with server tags into a namespace which does not
// allow them is refused with 108, which maps to HTTP 400, and stores nothing.
func TestHTTPPublishServerTagsNotAllowedRefused(t *testing.T) {
	server, api := serverTagsHTTPServer(t)

	status := postServerTagsRequest(t, server, "/publish", `{"channel":"plain:room","data":{},"server_tags":{"tenant":"t-17"}}`)
	require.Equal(t, http.StatusBadRequest, status)

	history := api.History(context.Background(), &HistoryRequest{Channel: "plain:room", Limit: -1})
	require.Nil(t, history.Error)
	require.Empty(t, history.Result.Publications)
}

// TestHTTPPublishServerTagsOverCapRefused checks a publish over HTTP over the tags cap is refused with 1000, which maps
// to HTTP 400, and stores nothing.
func TestHTTPPublishServerTagsOverCapRefused(t *testing.T) {
	server, api := serverTagsHTTPServer(t)

	status := postServerTagsRequest(t, server, "/publish", `{"channel":"caps:room","data":{},"tags":{"a":"1","b":"2"},"server_tags":{"c":"3","d":"4"}}`)
	require.Equal(t, http.StatusBadRequest, status)

	history := api.History(context.Background(), &HistoryRequest{Channel: "caps:room", Limit: -1})
	require.Nil(t, history.Error)
	require.Empty(t, history.Result.Publications)
}

// TestHTTPBroadcastServerTagsPerChannel checks a broadcast over HTTP with server tags publishes into the channel whose
// namespace allows them, and refuses the channel whose namespace does not, without refusing the whole request.
func TestHTTPBroadcastServerTagsPerChannel(t *testing.T) {
	server, api := serverTagsHTTPServer(t)

	status := postServerTagsRequest(t, server, "/broadcast", `{"channels":["st:a","plain:b"],"data":{},"server_tags":{"tenant":"t-17"}}`)
	require.Equal(t, http.StatusOK, status)

	history := api.History(context.Background(), &HistoryRequest{Channel: "st:a", Limit: -1})
	require.Nil(t, history.Error)
	require.Len(t, history.Result.Publications, 1)

	history = api.History(context.Background(), &HistoryRequest{Channel: "plain:b", Limit: -1})
	require.Nil(t, history.Error)
	require.Empty(t, history.Result.Publications)
}
