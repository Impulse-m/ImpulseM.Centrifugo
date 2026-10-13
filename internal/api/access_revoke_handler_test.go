package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/stretchr/testify/require"
)

func TestAPIHandlerAccessRevoke(t *testing.T) {
	apiExecutor, store, _ := newAccessRevokeExecutor(t, config.DefaultConfig(), 1)
	handler := NewHandler(apiExecutor.node, apiExecutor, Config{})

	// Successful call.
	request, _ := http.NewRequest(http.MethodPost, "/block_user", strings.NewReader(`{"user":"user"}`))
	recorder := httptest.NewRecorder()
	handler.Routes()["/block_user"](recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, accessrevoke.Blocked, userVerdict(store, "user"))

	// Rate limited call is answered with 429 in transport error mode and nothing is applied.
	request, _ = http.NewRequest(http.MethodPost, "/unblock_user", strings.NewReader(`{"user":"user"}`))
	request.Header.Set("X-Centrifugo-Error-Mode", "transport")
	recorder = httptest.NewRecorder()
	handler.Routes()["/unblock_user"](recorder, request)
	require.Equal(t, http.StatusTooManyRequests, recorder.Code)
	require.Equal(t, accessrevoke.Blocked, userVerdict(store, "user"))
}
