package client

import (
	"bytes"
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/proxy"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/cristalhq/jwt/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const (
	revokeTestUUID      = "5e1d6c2a-4f3b-4a8e-9c7d-0b1a2c3d4e5f"
	revokeTestOtherUUID = "0a9b8c7d-6e5f-4a3b-8c2d-1e0f9a8b7c6d"
)

var revokeTestRegistry = prometheus.NewRegistry()

func TestMain(m *testing.M) {
	_ = metrics.Init(metrics.Config{
		Registerer: revokeTestRegistry,
	})
	os.Exit(m.Run())
}

// getConnTokenWithClaims returns HMAC connection token with the given claims, signed with "secret".
func getConnTokenWithClaims(t *testing.T, claims map[string]any) string {
	t.Helper()
	signer, err := jwt.NewSignerHS(jwt.HS256, []byte("secret"))
	require.NoError(t, err)
	token, err := jwt.NewBuilder(signer).Build(claims)
	require.NoError(t, err)
	return token.String()
}

// rejectsCount returns value of access revocation rejects counter for given labels.
func rejectsCount(t *testing.T, reason string, stage string) float64 {
	t.Helper()
	families, err := revokeTestRegistry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "centrifugo_access_revoke_rejects_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["reason"] == reason && labels["stage"] == stage {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func newRevokeTestHandler(t *testing.T, cfg config.Config) (*Handler, *accessrevoke.Store, *centrifuge.Node) {
	t.Helper()
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	verifier, err := jwtverify.NewTokenVerifierJWT(jwtverify.VerifierConfig{
		HMACSecretKey:   "secret",
		GenerationClaim: "gen",
	}, cfgContainer)
	require.NoError(t, err)
	store := accessrevoke.NewStore(time.Minute)
	h := NewHandler(node, cfgContainer, verifier, nil, &ProxyMap{})
	h.SetAccessRevoke(store, nil)
	return h, store, node
}

func connectRevokeTestClient(t *testing.T, node *centrifuge.Node, token string) *centrifuge.Client {
	t.Helper()
	transport := tools.NewTestTransport()
	client, closeFn, err := centrifuge.NewClient(context.Background(), node, transport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn() })

	command := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{Token: token},
	}
	data, err := protocol.NewJSONCommandEncoder().Encode(command)
	require.NoError(t, err)
	require.True(t, centrifuge.HandleReadFrame(client, bytes.NewReader(data), 1<<20))
	return client
}

func TestAccessRevokeConnectBlockedUser(t *testing.T) {
	h, store, _ := newRevokeTestHandler(t, config.DefaultConfig())
	require.NoError(t, store.BlockUser("42", 0))
	before := rejectsCount(t, "blocked", "connect")

	_, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42"}),
	}, nil, false)
	require.Equal(t, accessrevoke.DisconnectUserBlocked, err)
	require.Equal(t, before+1, rejectsCount(t, "blocked", "connect"))
}

func TestAccessRevokeConnectRevokedSession(t *testing.T) {
	h, store, _ := newRevokeTestHandler(t, config.DefaultConfig())
	now := time.Now().Unix()
	require.NoError(t, store.RevokeSession(revokeTestUUID, now, 0))
	before := rejectsCount(t, "session", "connect")

	_, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42", "jti": revokeTestUUID, "iat": now - 10}),
	}, nil, false)
	require.ErrorIs(t, err, centrifuge.ErrorTokenExpired)
	require.Equal(t, before+1, rejectsCount(t, "session", "connect"))
}

func TestAccessRevokeConnectLoopGuard(t *testing.T) {
	h, store, _ := newRevokeTestHandler(t, config.DefaultConfig())
	now := time.Now().Unix()
	require.NoError(t, store.RevokeSession(revokeTestUUID, now-100, 0))
	before := rejectsCount(t, "loop_guard", "connect")

	// Token is issued after the revocation, auth service reissued it for a revoked session.
	_, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42", "jti": revokeTestUUID, "iat": now}),
	}, nil, false)
	require.Equal(t, centrifuge.DisconnectInvalidToken, err)
	require.Equal(t, before+1, rejectsCount(t, "loop_guard", "connect"))
}

func TestAccessRevokeConnectAllowedSetsTokenIdentity(t *testing.T) {
	h, store, _ := newRevokeTestHandler(t, config.DefaultConfig())
	now := time.Now().Unix()
	require.NoError(t, store.RevokeSession(revokeTestOtherUUID, now-100, 0))

	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42", "jti": revokeTestUUID, "iat": now - 10, "gen": 3}),
	}, nil, false)
	require.NoError(t, err)
	require.NotNil(t, reply.Credentials)
	require.Equal(t, accessrevoke.Identity(revokeTestUUID, 3, now-10), reply.Credentials.TokenIdentity)
}

func TestAccessRevokeConnectDisabledFeatures(t *testing.T) {
	now := time.Now().Unix()
	cfg := config.DefaultConfig()
	cfg.UserBlock.Enabled = false
	cfg.TokenRevoke.Enabled = false
	cfg.UserTokensInvalidate.Enabled = false
	h, store, _ := newRevokeTestHandler(t, cfg)
	require.NoError(t, store.BlockUser("42", 0))
	require.NoError(t, store.RevokeSession(revokeTestUUID, now, 0))
	require.NoError(t, store.InvalidateUser("42", 5, 0, now, 0))

	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42", "jti": revokeTestUUID, "iat": now - 10, "gen": 1}),
	}, nil, false)
	require.NoError(t, err)
	require.Equal(t, "42", reply.Credentials.UserID)
}

func TestAccessRevokeConnectProxyPathChecksBlockOnly(t *testing.T) {
	h, store, _ := newRevokeTestHandler(t, config.DefaultConfig())
	require.NoError(t, store.BlockUser("42", 0))
	require.NoError(t, store.InvalidateUser("43", 5, 0, time.Now().Unix(), 0))

	connectProxyHandler := func(user string) proxy.ConnectingHandlerFunc {
		return func(context.Context, centrifuge.ConnectEvent) (centrifuge.ConnectReply, proxy.ConnectExtra, error) {
			return centrifuge.ConnectReply{
				Credentials: &centrifuge.Credentials{UserID: user},
			}, proxy.ConnectExtra{}, nil
		}
	}

	_, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{}, connectProxyHandler("42"), true)
	require.Equal(t, accessrevoke.DisconnectUserBlocked, err)

	// Without a token only the block applies, so generation invalidation does not reject here.
	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{}, connectProxyHandler("43"), true)
	require.NoError(t, err)
	require.Equal(t, "43", reply.Credentials.UserID)
}

func TestAccessRevokeRefresh(t *testing.T) {
	now := time.Now().Unix()
	client := &tools.TestClientMock{
		IDFunc:     func() string { return "client-1" },
		UserIDFunc: func() string { return "42" },
	}

	tests := []struct {
		name          string
		setup         func(store *accessrevoke.Store)
		claims        map[string]any
		expectReply   func(t *testing.T, reply centrifuge.RefreshReply)
		expectedError error
	}{
		{
			name: "blocked user",
			setup: func(store *accessrevoke.Store) {
				require.NoError(t, store.BlockUser("42", 0))
			},
			claims:        map[string]any{"sub": "42", "jti": revokeTestUUID, "iat": now},
			expectedError: accessrevoke.DisconnectUserBlocked,
		},
		{
			name: "revoked session",
			setup: func(store *accessrevoke.Store) {
				require.NoError(t, store.RevokeSession(revokeTestUUID, now, 0))
			},
			claims: map[string]any{"sub": "42", "jti": revokeTestUUID, "iat": now - 10},
			expectReply: func(t *testing.T, reply centrifuge.RefreshReply) {
				require.True(t, reply.Expired)
			},
		},
		{
			name: "loop guard",
			setup: func(store *accessrevoke.Store) {
				require.NoError(t, store.RevokeSession(revokeTestUUID, now-100, 0))
			},
			claims:        map[string]any{"sub": "42", "jti": revokeTestUUID, "iat": now},
			expectedError: centrifuge.DisconnectInvalidToken,
		},
		{
			name:   "allowed updates token identity",
			setup:  func(_ *accessrevoke.Store) {},
			claims: map[string]any{"sub": "42", "jti": revokeTestUUID, "iat": now, "gen": 4},
			expectReply: func(t *testing.T, reply centrifuge.RefreshReply) {
				require.False(t, reply.Expired)
				require.NotNil(t, reply.TokenIdentity)
				require.Equal(t, accessrevoke.Identity(revokeTestUUID, 4, now), *reply.TokenIdentity)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, store, _ := newRevokeTestHandler(t, config.DefaultConfig())
			tt.setup(store)
			reply, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
				Token: getConnTokenWithClaims(t, tt.claims),
			}, nil)
			if tt.expectedError != nil {
				require.Equal(t, tt.expectedError, err)
				return
			}
			require.NoError(t, err)
			if tt.expectReply != nil {
				tt.expectReply(t, reply)
			}
		})
	}
}

func TestAccessRevokeRefreshProxyPathChecksBlockOnly(t *testing.T) {
	h, store, _ := newRevokeTestHandler(t, config.DefaultConfig())
	require.NoError(t, store.BlockUser("42", 0))

	proxyCalls := 0
	refreshProxyHandler := func(proxy.Client, centrifuge.RefreshEvent, proxy.PerCallData) (centrifuge.RefreshReply, proxy.RefreshExtra, error) {
		proxyCalls++
		return centrifuge.RefreshReply{}, proxy.RefreshExtra{}, nil
	}

	blockedClient := &tools.TestClientMock{
		IDFunc:     func() string { return "client-1" },
		UserIDFunc: func() string { return "42" },
	}
	_, _, err := h.OnRefresh(blockedClient, centrifuge.RefreshEvent{}, refreshProxyHandler)
	require.Equal(t, accessrevoke.DisconnectUserBlocked, err)
	require.Equal(t, 0, proxyCalls)

	activeClient := &tools.TestClientMock{
		IDFunc:     func() string { return "client-2" },
		UserIDFunc: func() string { return "43" },
	}
	require.NoError(t, store.RevokeSession(revokeTestUUID, time.Now().Unix(), 0))
	_, _, err = h.OnRefresh(activeClient, centrifuge.RefreshEvent{}, refreshProxyHandler)
	require.NoError(t, err)
	require.Equal(t, 1, proxyCalls)
}

func TestAccessRevokeRecheckConnection(t *testing.T) {
	now := time.Now().Unix()
	h, store, node := newRevokeTestHandler(t, config.DefaultConfig())
	require.NoError(t, h.Setup())

	client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, map[string]any{
		"sub": "42",
		"jti": revokeTestUUID,
		"iat": now,
	}))

	disconnect, revoked := h.recheckConnection(client)
	require.False(t, revoked)
	require.Equal(t, centrifuge.Disconnect{}, disconnect)

	// Session revoked after connect: the recheck decides with live disconnect code.
	require.NoError(t, store.RevokeSession(revokeTestUUID, now, 0))
	disconnect, revoked = h.recheckConnection(client)
	require.True(t, revoked)
	require.Equal(t, centrifuge.DisconnectExpired, disconnect)

	// Block takes precedence over session revocation.
	require.NoError(t, store.BlockUser("42", 0))
	disconnect, revoked = h.recheckConnection(client)
	require.True(t, revoked)
	require.Equal(t, accessrevoke.DisconnectUserBlocked, disconnect)
}

// fakeAccessSource is an accessrevoke.Source with settable readiness and lag.
type fakeAccessSource struct {
	ready   atomic.Bool
	lagging atomic.Bool
}

func (s *fakeAccessSource) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (s *fakeAccessSource) Ready() bool {
	return s.ready.Load()
}

func (s *fakeAccessSource) Lagging() bool {
	return s.lagging.Load()
}

func TestAccessRevokeConnectSourceNotReady(t *testing.T) {
	h, _, _ := newRevokeTestHandler(t, config.DefaultConfig())
	source := &fakeAccessSource{}
	h.accessSource = source
	before := rejectsCount(t, "not_ready", "connect")

	// Token is invalid: the gate must answer before token verification.
	_, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: "invalid",
	}, nil, false)
	require.Equal(t, accessrevoke.ErrorNotReady, err)
	require.Equal(t, before+1, rejectsCount(t, "not_ready", "connect"))

	source.ready.Store(true)
	_, err = h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42"}),
	}, nil, false)
	require.NoError(t, err)
}

func TestAccessRevokeConnectSourceLaggingRejectsOnlyWhenStrict(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.AccessRevoke.Source.RejectConnectOnLag = false
	h, _, _ := newRevokeTestHandler(t, cfg)
	source := &fakeAccessSource{}
	source.ready.Store(true)
	source.lagging.Store(true)
	h.accessSource = source
	connectEvent := centrifuge.ConnectEvent{
		Token: getConnTokenWithClaims(t, map[string]any{"sub": "42"}),
	}

	_, err := h.OnClientConnecting(context.Background(), connectEvent, nil, false)
	require.NoError(t, err)

	cfg.AccessRevoke.Source.RejectConnectOnLag = true
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	h.cfgContainer = cfgContainer
	before := rejectsCount(t, "lagging", "connect")

	_, err = h.OnClientConnecting(context.Background(), connectEvent, nil, false)
	require.Equal(t, accessrevoke.ErrorNotReady, err)
	require.Equal(t, before+1, rejectsCount(t, "lagging", "connect"))

	source.lagging.Store(false)
	_, err = h.OnClientConnecting(context.Background(), connectEvent, nil, false)
	require.NoError(t, err)
}

func TestAccessRevokeRecheckConnectionLoopGuard(t *testing.T) {
	now := time.Now().Unix()
	h, store, node := newRevokeTestHandler(t, config.DefaultConfig())
	require.NoError(t, h.Setup())

	client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, map[string]any{
		"sub": "42",
		"jti": revokeTestUUID,
		"iat": now,
	}))
	require.NoError(t, store.RevokeSession(revokeTestUUID, now-100, 0))

	disconnect, revoked := h.recheckConnection(client)
	require.True(t, revoked)
	require.Equal(t, centrifuge.DisconnectInvalidToken, disconnect)
}

func TestAccessRevokeRecheckConnectionWithoutTokenIdentityChecksBlockOnly(t *testing.T) {
	now := time.Now().Unix()
	h, store, node := newRevokeTestHandler(t, config.DefaultConfig())
	require.NoError(t, h.Setup())

	// Token without jti, iat and generation gives zero token identity, as connections of the connect proxy do.
	client := connectRevokeTestClient(t, node, getConnTokenWithClaims(t, map[string]any{"sub": "44"}))
	require.NoError(t, store.InvalidateUser("44", 5, 0, now, 0))

	_, revoked := h.recheckConnection(client)
	require.False(t, revoked)

	require.NoError(t, store.BlockUser("44", 0))
	disconnect, revoked := h.recheckConnection(client)
	require.True(t, revoked)
	require.Equal(t, accessrevoke.DisconnectUserBlocked, disconnect)
}
