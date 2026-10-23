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

// blockingSource is a token source whose fetch waits until release is closed. It ignores any context, as the source of
// x/oauth2 does for a token request.
type blockingSource struct {
	release chan struct{}
}

func (s *blockingSource) Token() (*oauth2.Token, error) {
	<-s.release
	return &oauth2.Token{
		AccessToken: "access",
		Expiry:      time.Now().Add(time.Hour),
	}, nil
}

// TestTokenGateWaitEndsAtShutdown checks that a caller waiting for a fetch that does not return is released by the
// shutdown: the wait ends with the shutdown error, and the caller does not wait for the fetch.
func TestTokenGateWaitEndsAtShutdown(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	shutdown, stop := context.WithCancel(context.Background())
	gate := newShutdownTokenGate(&blockingSource{release: release}, shutdown)

	errChan := make(chan error, 1)
	go func() {
		_, err := gate.Token(context.Background())
		errChan <- err
	}()
	stop()

	require.ErrorIs(t, <-errChan, context.Canceled)
}
