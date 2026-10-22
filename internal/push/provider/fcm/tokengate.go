package fcm

import (
	"context"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/sync/singleflight"
)

// tokenMinLeft is the time a cached access token must still have to be returned without a fetch.
const tokenMinLeft = 60 * time.Second

// fetchKey is the singleflight key of the one fetch of a credential.
const fetchKey = "fcm-token"

// tokenGate wraps the token source of a credential. It returns the cached token while it has time left, and runs at
// most one fetch at a time: concurrent callers share the result of that fetch. A caller waits no longer than its own
// context, and the fetch itself ends within the HTTP client timeout of the token source.
type tokenGate struct {
	source oauth2.TokenSource

	mu    sync.Mutex
	token *oauth2.Token

	group singleflight.Group
}

// newTokenGate returns a gate over source.
func newTokenGate(source oauth2.TokenSource) *tokenGate {
	return &tokenGate{
		source: source,
	}
}

// cached returns the cached token when it has time left, nil otherwise.
func (g *tokenGate) cached() *oauth2.Token {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.token == nil {
		return nil
	}
	if !g.token.Expiry.IsZero() && time.Until(g.token.Expiry) <= tokenMinLeft {
		return nil
	}
	return g.token
}

// Token returns an access token for a call. It returns the context error when ctx ends before the token is ready,
// and then no provider call may be made.
func (g *tokenGate) Token(ctx context.Context) (*oauth2.Token, error) {
	if token := g.cached(); token != nil {
		return token, nil
	}

	resultChan := g.group.DoChan(fetchKey, func() (any, error) {
		token, err := g.source.Token()
		if err != nil {
			return nil, err
		}

		g.mu.Lock()
		g.token = token
		g.mu.Unlock()
		return token, nil
	})

	select {
	case result := <-resultChan:
		if result.Err != nil {
			return nil, result.Err
		}
		return result.Val.(*oauth2.Token), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
