package api

import (
	"context"
	"strings"
	"testing"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/stretchr/testify/require"
)

func TestUnsubscribeAPICode(t *testing.T) {
	node := nodeWithMemoryEngine()
	cfg := config.DefaultConfig()
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)

	api := NewExecutor(node, cfgContainer, &testSurveyCaller{}, ExecutorConfig{Protocol: "test", UseOpenTelemetry: false})
	tests := []struct {
		name        string
		unsubscribe *Unsubscribe
		// bad is true when the request must be refused as a bad request.
		bad bool
	}{
		{
			name: "lowest terminal code",
			unsubscribe: &Unsubscribe{
				Code:   2100,
				Reason: "terminal",
			},
		},
		{
			name: "highest terminal code",
			unsubscribe: &Unsubscribe{
				Code: 2499,
			},
		},
		{
			name: "lowest resubscribe code",
			unsubscribe: &Unsubscribe{
				Code:   2600,
				Reason: "resubscribe",
			},
		},
		{
			name: "highest resubscribe code",
			unsubscribe: &Unsubscribe{
				Code: 2999,
			},
		},
		{
			name: "code below terminal range",
			unsubscribe: &Unsubscribe{
				Code: 2050,
			},
			bad: true,
		},
		{
			name: "code 2500 is reserved",
			unsubscribe: &Unsubscribe{
				Code: 2500,
			},
			bad: true,
		},
		{
			name: "code in reserved range",
			unsubscribe: &Unsubscribe{
				Code: 2550,
			},
			bad: true,
		},
		{
			name: "code above resubscribe range",
			unsubscribe: &Unsubscribe{
				Code: 3000,
			},
			bad: true,
		},
		{
			name: "reason without code",
			unsubscribe: &Unsubscribe{
				Reason: "reason",
			},
			bad: true,
		},
		{
			name: "reason at limit",
			unsubscribe: &Unsubscribe{
				Code:   2100,
				Reason: strings.Repeat("r", 123),
			},
		},
		{
			name: "reason above limit",
			unsubscribe: &Unsubscribe{
				Code:   2100,
				Reason: strings.Repeat("r", 124),
			},
			bad: true,
		},
		{
			name:        "code 0 with empty reason is absent",
			unsubscribe: &Unsubscribe{},
		},
		{
			name:        "absent unsubscribe",
			unsubscribe: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := api.Unsubscribe(context.Background(), &UnsubscribeRequest{
				User:        "test",
				Channel:     "test",
				Unsubscribe: tt.unsubscribe,
			})
			if tt.bad {
				require.Equal(t, ErrorBadRequest, resp.Error)
				return
			}
			require.Nil(t, resp.Error)
		})
	}
}

func TestSanitizeUnsubscribeBody(t *testing.T) {
	tests := []struct {
		name string
		body string
		// want is the body after sanitizing, compared as JSON.
		want string
		// same is true when the body must be returned as it is.
		same bool
	}{
		{
			name: "valid code",
			body: `{"channel":"c","user":"u","unsubscribe":{"code":2600,"reason":"r"}}`,
			same: true,
		},
		{
			name: "absent unsubscribe",
			body: `{"channel":"c","user":"u"}`,
			same: true,
		},
		{
			name: "code 0 with empty reason",
			body: `{"channel":"c","unsubscribe":{}}`,
			same: true,
		},
		{
			name: "invalid code",
			body: `{"channel":"c","user":"u","unsubscribe":{"code":2050}}`,
			want: `{"channel":"c","user":"u"}`,
		},
		{
			name: "invalid reason",
			body: `{"channel":"c","unsubscribe":{"code":2100,"reason":"` + strings.Repeat("r", 124) + `"}}`,
			want: `{"channel":"c"}`,
		},
		{
			name: "key without case",
			body: `{"channel":"c","Unsubscribe":{"code":3000}}`,
			want: `{"channel":"c"}`,
		},
		{
			name: "body is not an object",
			body: `[1]`,
			same: true,
		},
		{
			name: "body is not json",
			body: `not json`,
			same: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeUnsubscribeBody([]byte(tt.body))
			if tt.same {
				require.Equal(t, tt.body, string(got))
				return
			}
			require.JSONEq(t, tt.want, string(got))
		})
	}
}
