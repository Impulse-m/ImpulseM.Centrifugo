package health

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

func TestHealthHandler(t *testing.T) {
	node := nodeWithMemoryEngine()
	h := NewHandler(node, Config{})

	ts := httptest.NewServer(h)
	defer ts.Close()

	res, err := http.Get(ts.URL)
	require.NoError(t, err)
	require.Equal(t, res.StatusCode, http.StatusOK)
	defer func() { _ = res.Body.Close() }()

	require.Equal(t, "application/json", res.Header.Get("Content-Type"))

	data, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, []byte(`{}`), data)
}

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

func nodeWithMemoryEngine() *centrifuge.Node {
	n, err := centrifuge.New(centrifuge.Config{})
	if err != nil {
		panic(err)
	}
	err = n.Run()
	if err != nil {
		panic(err)
	}
	return n
}
