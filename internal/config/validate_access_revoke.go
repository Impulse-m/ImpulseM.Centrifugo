package config

import (
	"fmt"
	"slices"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

func validateEd25519PublicKeys(name string, token configtypes.Token) error {
	if len(token.Ed25519PublicKeys) == 0 {
		return nil
	}
	if token.JWKSPublicEndpoint != "" {
		return fmt.Errorf("in %s: ed25519_public_keys can not be used together with jwks_public_endpoint", name)
	}
	seenKIDs := make(map[string]struct{}, len(token.Ed25519PublicKeys))
	for i, key := range token.Ed25519PublicKeys {
		if key.PublicKey == "" {
			return fmt.Errorf("in %s: ed25519_public_keys[%d]: public_key is required", name, i)
		}
		if key.KID == "" && len(token.Ed25519PublicKeys) > 1 {
			return fmt.Errorf("in %s: ed25519_public_keys[%d]: kid is required when more than one key is configured", name, i)
		}
		if _, ok := seenKIDs[key.KID]; ok {
			return fmt.Errorf("in %s: ed25519_public_keys[%d]: duplicate kid \"%s\"", name, i, key.KID)
		}
		seenKIDs[key.KID] = struct{}{}
	}
	return nil
}

var knownAccessRevokeStorageTypes = []string{"", "memory"}

func validateAccessRevoke(cfg Config) error {
	if !slices.Contains(knownAccessRevokeStorageTypes, cfg.UserBlock.StorageType) {
		return fmt.Errorf("unknown user_block.storage_type: \"%s\"", cfg.UserBlock.StorageType)
	}
	if !slices.Contains(knownAccessRevokeStorageTypes, cfg.TokenRevoke.StorageType) {
		return fmt.Errorf("unknown token_revoke.storage_type: \"%s\"", cfg.TokenRevoke.StorageType)
	}
	if !slices.Contains(knownAccessRevokeStorageTypes, cfg.UserTokensInvalidate.StorageType) {
		return fmt.Errorf("unknown user_tokens_invalidate.storage_type: \"%s\"", cfg.UserTokensInvalidate.StorageType)
	}
	if cfg.TokenRevoke.UIDClaim != "" && !customClaimRe.MatchString(cfg.TokenRevoke.UIDClaim) {
		return fmt.Errorf("invalid token_revoke.uid_claim: %s, must match %s regular expression", cfg.TokenRevoke.UIDClaim, customClaimRe.String())
	}
	if cfg.UserTokensInvalidate.GenerationClaim != "" && !customClaimRe.MatchString(cfg.UserTokensInvalidate.GenerationClaim) {
		return fmt.Errorf("invalid user_tokens_invalidate.generation_claim: %s, must match %s regular expression", cfg.UserTokensInvalidate.GenerationClaim, customClaimRe.String())
	}
	if cfg.AccessRevoke.DisconnectRate <= 0 {
		return fmt.Errorf("access_revoke.disconnect_rate must be greater than zero")
	}
	if cfg.AccessRevoke.DisconnectQueueSize <= 0 {
		return fmt.Errorf("access_revoke.disconnect_queue_size must be greater than zero")
	}
	if cfg.AccessRevoke.NotifyRate <= 0 {
		return fmt.Errorf("access_revoke.notify_rate must be greater than zero")
	}
	if cfg.AccessRevoke.DefaultTTL.ToDuration() <= 0 {
		return fmt.Errorf("access_revoke.default_ttl must be greater than zero")
	}
	if cfg.AccessRevoke.MaxTTL.ToDuration() < 0 {
		return fmt.Errorf("access_revoke.max_ttl must not be negative, use 0 to turn the limit off")
	}
	if cfg.AccessRevoke.SweepInterval.ToDuration() <= 0 {
		return fmt.Errorf("access_revoke.sweep_interval must be greater than zero")
	}
	return validateAccessRevokeSource(cfg)
}

var knownAccessRevokeSourceTypes = []string{"", "kafka"}

func validateAccessRevokeSource(cfg Config) error {
	source := cfg.AccessRevoke.Source
	if !slices.Contains(knownAccessRevokeSourceTypes, source.Type) {
		return fmt.Errorf("unknown access_revoke.source.type: \"%s\"", source.Type)
	}
	if source.Type == "" {
		return nil
	}
	if source.Window.ToDuration() <= 0 {
		return fmt.Errorf("access_revoke.source.window must be greater than zero")
	}
	if source.StartupTimeout.ToDuration() <= 0 {
		return fmt.Errorf("access_revoke.source.startup_timeout must be greater than zero")
	}
	if source.LagThreshold.ToDuration() <= 0 {
		return fmt.Errorf("access_revoke.source.lag_threshold must be greater than zero")
	}
	if source.Window.ToDuration() < cfg.AccessRevoke.DefaultTTL.ToDuration() {
		return fmt.Errorf("access_revoke.source.window must be greater or equal to access_revoke.default_ttl")
	}
	if source.Type == "kafka" {
		if len(source.Kafka.Brokers) == 0 {
			return fmt.Errorf("access_revoke.source.kafka.brokers must be set for kafka source")
		}
		if source.Kafka.Topic == "" {
			return fmt.Errorf("access_revoke.source.kafka.topic must be set for kafka source")
		}
		if source.Kafka.FetchMaxWait.ToDuration() <= 0 {
			return fmt.Errorf("access_revoke.source.kafka.fetch_max_wait must be greater than zero")
		}
		if source.Kafka.AssumeRoleARN != "" && source.Kafka.SASLMechanism != "aws-msk-iam" {
			return fmt.Errorf("access_revoke.source.kafka.assume_role_arn requires sasl_mechanism aws-msk-iam")
		}
	}
	return nil
}
