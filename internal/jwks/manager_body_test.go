package jwks

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLoadDataRejectsOversizedBody serves a JWKS body of 2 MiB. The body is over the size limit, so the load fails
// instead of reading it all.
func TestLoadDataRejectsOversizedBody(t *testing.T) {
	oversized := bytes.Repeat([]byte("a"), 2<<20)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(oversized)
	}))
	defer ts.Close()

	manager, err := NewManager(ts.URL)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL, nil)
	require.NoError(t, err)

	_, err = manager.loadData(req)
	require.ErrorIs(t, err, errBodyTooLarge)
}
