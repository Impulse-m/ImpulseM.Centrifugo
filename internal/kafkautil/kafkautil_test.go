package kafkautil

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestOptionsPlainBrokers(t *testing.T) {
	opts, err := Options(Config{
		Name:    "test",
		Brokers: []string{"localhost:9092"},
	}, zerolog.Nop())
	require.NoError(t, err)
	// Seed brokers and logger are always set.
	require.Len(t, opts, 2)
}

func TestOptionsSASLMechanisms(t *testing.T) {
	for _, mechanism := range []string{"plain", "scram-sha-256", "scram-sha-512", "aws-msk-iam"} {
		t.Run(mechanism, func(t *testing.T) {
			opts, err := Options(Config{
				Name:          "test",
				Brokers:       []string{"localhost:9092"},
				SASLMechanism: mechanism,
				SASLUser:      "user",
				SASLPassword:  "password",
			}, zerolog.Nop())
			require.NoError(t, err)
			require.Len(t, opts, 3)
		})
	}
}

func TestOptionsUnsupportedSASLMechanism(t *testing.T) {
	_, err := Options(Config{
		Name:          "test",
		Brokers:       []string{"localhost:9092"},
		SASLMechanism: "unknown",
	}, zerolog.Nop())
	require.ErrorContains(t, err, "unsupported SASL mechanism: unknown")
}

func TestOptionsTLSError(t *testing.T) {
	_, err := Options(Config{
		Name:    "test",
		Brokers: []string{"localhost:9092"},
		TLS: configtypes.TLSConfig{
			Enabled: true,
			CertPem: "/nonexistent/cert.pem",
			KeyPem:  "/nonexistent/key.pem",
		},
	}, zerolog.Nop())
	require.ErrorContains(t, err, "error making TLS configuration")
}
