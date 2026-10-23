package fcm

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

// countingSource is a token source that fails on its first calls and then answers a token. It counts its calls.
type countingSource struct {
	calls    atomic.Int64
	failures int64
}

func (s *countingSource) Token() (*oauth2.Token, error) {
	calls := s.calls.Add(1)
	if calls <= s.failures {
		return nil, errors.New("token endpoint is down")
	}
	return &oauth2.Token{
		AccessToken: "access",
		Expiry:      time.Now().Add(time.Hour),
	}, nil
}

func TestTokenGateFailureIsNotHeld(t *testing.T) {
	source := &countingSource{failures: 1}
	gate := newTokenGate(source)

	_, err := gate.Token(context.Background())
	require.Error(t, err)

	// The next call starts a new fetch at once: a failed fetch is not held for any time.
	token, err := gate.Token(context.Background())
	require.NoError(t, err)
	require.Equal(t, "access", token.AccessToken)
	require.Equal(t, int64(2), source.calls.Load())
}

func TestTokenGateCachesToken(t *testing.T) {
	source := &countingSource{}
	gate := newTokenGate(source)

	for index := 0; index < 10; index++ {
		_, err := gate.Token(context.Background())
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), source.calls.Load())
}
