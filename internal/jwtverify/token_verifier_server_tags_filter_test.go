package jwtverify

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/centrifugal/protocol"
	"github.com/cristalhq/jwt/v5"
	"github.com/stretchr/testify/require"
)

func Test_tokenVerifierJWT_ServerTagsFilter(t *testing.T) {
	cfg := config.DefaultConfig()
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)

	verifierJWT, err := NewTokenVerifierJWT(VerifierConfig{HMACSecretKey: "secret"}, cfgContainer)
	require.NoError(t, err)

	signer, err := jwt.NewSignerHS(jwt.HS256, []byte("secret"))
	require.NoError(t, err)
	builder := jwt.NewBuilder(signer)

	filter := &protocol.FilterNode{
		Op: "and",
		Nodes: []*protocol.FilterNode{
			{Key: "type", Cmp: "eq", Val: "a"},
			{Key: "tier", Cmp: "in", Vals: []string{"x", "y"}},
		},
	}
	exp := jwt.NewNumericDate(time.Now().Add(time.Hour))

	connClaims := &ConnectTokenClaims{
		Subs: map[string]SubscribeOptions{
			"channel1": {
				ServerTagsFilter: filter,
			},
		},
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user1",
			ExpiresAt: exp,
		},
	}
	connToken, err := builder.Build(connClaims)
	require.NoError(t, err)

	connectToken, err := verifierJWT.VerifyConnectToken(connToken.String(), false)
	require.NoError(t, err)
	require.Contains(t, connectToken.Subs, "channel1")
	require.Equal(t, filter, connectToken.Subs["channel1"].ServerTagsFilter)

	subClaims := &SubscribeTokenClaims{
		SubscribeOptions: SubscribeOptions{
			ServerTagsFilter: filter,
		},
		Channel: "channel1",
		Client:  "client1",
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user1",
			ExpiresAt: exp,
		},
	}
	subToken, err := builder.Build(subClaims)
	require.NoError(t, err)

	subscribeToken, err := verifierJWT.VerifySubscribeToken(subToken.String(), false)
	require.NoError(t, err)
	require.Equal(t, filter, subscribeToken.Options.ServerTagsFilter)
}
