package configtypes

import (
	"encoding/json"
	"fmt"
)

// Ed25519PublicKey is a static Ed25519 public key used to validate EdDSA tokens.
type Ed25519PublicKey struct {
	// KID is a key ID. EdDSA token selects the key by its kid header. Required when more than one key is configured.
	KID string `mapstructure:"kid" json:"kid" envconfig:"kid" yaml:"kid" toml:"kid" expose:"full" doc:"A key ID. EdDSA token selects the key by its kid header. Required when more than one key is configured."`
	// PublicKey is a PEM encoded Ed25519 public key (PKIX "PUBLIC KEY").
	PublicKey string `mapstructure:"public_key" json:"public_key" envconfig:"public_key" yaml:"public_key" toml:"public_key" expose:"full" doc:"A PEM encoded Ed25519 public key (PKIX 'PUBLIC KEY')."`
}

type Ed25519PublicKeys []Ed25519PublicKey

// Decode to implement the envconfig.Decoder interface
func (d *Ed25519PublicKeys) Decode(value string) error {
	if value == "" {
		// If the value is empty, leave the slice as it is.
		return nil
	}
	if err := json.Unmarshal([]byte(value), d); err != nil {
		return fmt.Errorf("failed to unmarshal JSON array: %w", err)
	}
	return nil
}
