package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/clientaddr"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestAuthThrottleUsesResolvedAddress(t *testing.T) {
	// Client address is enabled with nothing trusted: a private peer's rotating X-Forwarded-For
	// must not split the throttle key, which is the socket peer.
	resolver, err := clientaddr.New(configtypes.ClientAddress{Enabled: true, Header: "x-forwarded-for"})
	require.NoError(t, err)
	h := clientaddr.Middleware(resolver)(
		NewAuthThrottle(3, time.Minute, nil).Middleware(throttleTestHandler()),
	)

	req := func(spoofedIP string) int {
		r := httptest.NewRequest(http.MethodPost, "/auth?pw=wrong", nil)
		r.RemoteAddr = "10.0.0.9:12345"
		r.Header.Set("X-Forwarded-For", spoofedIP)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	require.Equal(t, http.StatusBadRequest, req("1.1.1.1"))
	require.Equal(t, http.StatusBadRequest, req("2.2.2.2"))
	require.Equal(t, http.StatusBadRequest, req("3.3.3.3"))
	require.Equal(t, http.StatusTooManyRequests, req("4.4.4.4"))
}
