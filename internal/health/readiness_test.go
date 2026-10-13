package health

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHealthHandlerReadiness(t *testing.T) {
	node := nodeWithMemoryEngine()
	var ready atomic.Bool
	h := NewHandler(node, Config{Ready: ready.Load})

	ts := httptest.NewServer(h)
	defer ts.Close()

	res, err := http.Get(ts.URL)
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
	data, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, []byte(`{"ready":false}`), data)

	ready.Store(true)
	res, err = http.Get(ts.URL)
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()
	require.Equal(t, http.StatusOK, res.StatusCode)
	data, err = io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, []byte(`{}`), data)
}
