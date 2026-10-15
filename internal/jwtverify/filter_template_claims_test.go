package jwtverify

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/filtertemplate"

	"github.com/cristalhq/jwt/v5"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const filterTemplateTestSecret = "secret"

func filterTemplateTestVerifier(t testing.TB) *VerifierJWT {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{
			Name: "chat",
			ChannelOptions: configtypes.ChannelOptions{
				ServerTagsFilterTemplate: configtypes.ServerTagsFilterTemplate{
					Filter: configtypes.TagsFilterNode{"key": "session", "cmp": "neq", "val": "{{claims.sid}}"},
				},
			},
		},
	}
	cfg.Channel.WithoutNamespace.ServerTagsFilterTemplate = configtypes.ServerTagsFilterTemplate{
		Filter: configtypes.TagsFilterNode{"key": "tenant", "cmp": "eq", "val": "{{claims.org.tenant}}"},
	}
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	verifier, err := NewTokenVerifierJWT(VerifierConfig{HMACSecretKey: filterTemplateTestSecret}, cfgContainer)
	require.NoError(t, err)
	return verifier
}

func signedConnectToken(t *testing.T, claims map[string]any) string {
	t.Helper()
	signer, err := jwt.NewSignerHS(jwt.HS256, []byte(filterTemplateTestSecret))
	require.NoError(t, err)
	builder := jwt.NewBuilder(signer)
	claims["sub"] = "user1"
	claims["exp"] = time.Now().Add(time.Hour).Unix()
	token, err := builder.Build(claims)
	require.NoError(t, err)
	return token.String()
}

func TestFilterTemplateClaimsValues(t *testing.T) {
	verifier := filterTemplateTestVerifier(t)
	token := signedConnectToken(t, map[string]any{
		"sid": "session-1",
		"org": map[string]any{"tenant": "acme"},
	})
	connectToken, err := verifier.VerifyConnectToken(token, false)
	require.NoError(t, err)

	claims := connectToken.FilterTemplateClaims
	require.Equal(t, []string{"org.tenant", "sid"}, claims.Paths)
	require.Equal(t, []string{"acme", "session-1"}, claims.Values)
	// The paths are the slice of the config, not a copy.
	configPaths := verifier.cfgContainer.ConfigRef().ServerTagsFilterClaims()
	require.Same(t, &configPaths[0], &claims.Paths[0])
}

func TestFilterTemplateClaimsMissingValues(t *testing.T) {
	verifier := filterTemplateTestVerifier(t)
	tests := []struct {
		name   string
		claims map[string]any
	}{
		{name: "absent", claims: map[string]any{"org": map[string]any{"tenant": "acme"}}},
		{name: "null", claims: map[string]any{"sid": nil, "org": map[string]any{"tenant": "acme"}}},
		{name: "object", claims: map[string]any{"sid": map[string]any{"v": 1}, "org": map[string]any{"tenant": "acme"}}},
		{name: "array", claims: map[string]any{"sid": []any{"a"}, "org": map[string]any{"tenant": "acme"}}},
		{name: "empty string", claims: map[string]any{"sid": "", "org": map[string]any{"tenant": "acme"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connectToken, err := verifier.VerifyConnectToken(signedConnectToken(t, tt.claims), false)
			require.NoError(t, err)
			require.Equal(t, []string{"org.tenant", "sid"}, connectToken.FilterTemplateClaims.Paths)
			require.Equal(t, "acme", connectToken.FilterTemplateClaims.Values[0])
			require.Equal(t, "", connectToken.FilterTemplateClaims.Values[1])
		})
	}
}

func TestFilterTemplateClaimsScalarTypes(t *testing.T) {
	verifier := filterTemplateTestVerifier(t)
	// A 64-bit integer keeps its digits, and a boolean is its text.
	token := signedConnectToken(t, map[string]any{
		"sid": int64(9007199254740993),
		"org": map[string]any{"tenant": true},
	})
	connectToken, err := verifier.VerifyConnectToken(token, false)
	require.NoError(t, err)
	require.Equal(t, []string{"true", "9007199254740993"}, connectToken.FilterTemplateClaims.Values)
}

func TestFilterTemplateClaimsWithoutTemplates(t *testing.T) {
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	verifier, err := NewTokenVerifierJWT(VerifierConfig{HMACSecretKey: filterTemplateTestSecret}, cfgContainer)
	require.NoError(t, err)

	token := signedConnectToken(t, map[string]any{"sid": "session-1"})
	connectToken, err := verifier.VerifyConnectToken(token, false)
	require.NoError(t, err)
	require.Equal(t, filtertemplate.Claims{}, connectToken.FilterTemplateClaims)

	raw := []byte(`{"sid":"session-1"}`)
	allocs := testing.AllocsPerRun(1000, func() {
		_ = verifier.filterTemplateClaims(raw)
	})
	require.Equal(t, float64(0), allocs)
}

func BenchmarkFilterTemplateClaims(b *testing.B) {
	verifier := filterTemplateTestVerifier(b)
	raw := []byte(`{"sub":"user1","sid":"session-1","org":{"tenant":"acme"}}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = verifier.filterTemplateClaims(raw)
	}
}

func TestScalarClaimValueKeepsNumberText(t *testing.T) {
	// A number keeps the text it has in the token, so 1.50 is not 1.5 and 1e3 is not 1000.
	cases := map[string]string{
		`1.50`:             "1.50",
		`1e3`:              "1e3",
		`9007199254740993`: "9007199254740993",
		`-0.25`:            "-0.25",
		`true`:             "true",
		`"text"`:           "text",
		`null`:             "",
		`{"a":1}`:          "",
		`[1,2]`:            "",
	}
	for raw, want := range cases {
		require.Equal(t, want, scalarClaimValue(gjson.Parse(raw)), raw)
	}
}
