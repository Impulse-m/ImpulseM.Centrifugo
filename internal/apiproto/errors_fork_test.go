package apiproto

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// TestErrorTooManyRequestsMapping checks the rate limit error keeps its code,
// message and HTTP and gRPC statuses that our call sites rely on.
func TestErrorTooManyRequestsMapping(t *testing.T) {
	require.Equal(t, uint32(111), ErrorTooManyRequests.Code)
	require.Equal(t, "too many requests", ErrorTooManyRequests.Message)
	require.Equal(t, http.StatusTooManyRequests, MapErrorToHTTPCode(ErrorTooManyRequests))
	require.Equal(t, codes.ResourceExhausted, MapErrorToGRPCCode(ErrorTooManyRequests))
}
