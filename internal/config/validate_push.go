package config

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// pushKeyPrefix is the path prefix of every push notification key.
const pushKeyPrefix string = "push_notifications."

// pushEnvPrefix is the environment prefix of every push notification key.
const pushEnvPrefix string = "CENTRIFUGO_PUSH_NOTIFICATIONS_"

// pushProviderAPNs is the provider name of APNs.
const pushProviderAPNs string = "apns"

// pushProviderFCM is the provider name of FCM.
const pushProviderFCM string = "fcm"

// maxPushRecipientItems is the largest number of recipients one send call may carry.
const maxPushRecipientItems int = 1000

// maxPushRequestBytes is the largest value of max_request_bytes: 16 MiB.
const maxPushRequestBytes int = 16 << 20

// maxPushConcurrentRequests is the largest value of max_concurrent_requests. Each running call holds
// its request in memory, so the worst case is this value times max_request_bytes.
const maxPushConcurrentRequests int = 256

// maxPushTokensBatchSize is the largest value of tokens_batch_size of a provider.
const maxPushTokensBatchSize int = 1000

// maxPushMaxInflight is the largest value of max_inflight of a provider.
const maxPushMaxInflight int = 10000

// maxPushTimeout is the largest value of sync_timeout and of providers.request_timeout.
const maxPushTimeout time.Duration = time.Minute

// knownPushProviders are the providers this build sends to.
var knownPushProviders []string = []string{pushProviderAPNs, pushProviderFCM}

// refusedPushProviders are provider names that the configuration model knows of but this build
// does not send to. They are refused at startup rather than accepted and never used.
var refusedPushProviders []string = []string{"hms", "webpush"}

// knownAPNsEndpoints are the APNs hosts.
var knownAPNsEndpoints []string = []string{"production", "development"}

// checkPushUnknownKeys refuses a push notification key that this build does not have, when push
// notifications are enabled. A key is refused both in the configuration file and in the environment.
// unknownEnvs is the list of unknown CENTRIFUGO_* variables from checkEnvironmentVars, which already
// skips the variables that Kubernetes injects; only the push variables of that list are refused here.
// Unknown keys of the rest of the configuration keep their warning. The first refused key in sorted
// order is named.
func checkPushUnknownKeys(conf Config, unknownKeys []string, unknownEnvs []string) error {
	if !conf.PushNotifications.Enabled {
		return nil
	}
	refused := withPrefix(unknownKeys, pushKeyPrefix)
	if len(refused) > 0 {
		slices.Sort(refused)
		return fmt.Errorf("%s is not available in this build", refused[0])
	}
	refusedEnvs := withPrefix(unknownEnvs, pushEnvPrefix)
	if len(refusedEnvs) == 0 {
		return nil
	}
	slices.Sort(refusedEnvs)
	return fmt.Errorf("%s is not available in this build", refusedEnvs[0])
}

// withPrefix returns the names that start with prefix, in their order.
func withPrefix(names []string, prefix string) []string {
	var matched []string
	for _, name := range names {
		if strings.HasPrefix(name, prefix) {
			matched = append(matched, name)
		}
	}
	return matched
}

// validatePushNotifications checks the push notification settings. Nothing is checked while the
// subsystem is disabled. The values of the settings are checked whenever it is enabled; the settings
// that a provider needs are checked only when that provider is enabled.
func validatePushNotifications(conf Config) error {
	push := conf.PushNotifications
	if !push.Enabled {
		return nil
	}
	if err := checkPushDuration("push_notifications.sync_timeout", push.SyncTimeout, maxPushTimeout); err != nil {
		return err
	}
	if err := checkPushDuration("push_notifications.providers.request_timeout", push.Providers.RequestTimeout, maxPushTimeout); err != nil {
		return err
	}
	if push.MaxRecipientItems < 1 || push.MaxRecipientItems > maxPushRecipientItems {
		return fmt.Errorf("push_notifications.max_recipient_items must be between 1 and %d, got %d", maxPushRecipientItems, push.MaxRecipientItems)
	}
	if err := checkPushIntRange("push_notifications.max_request_bytes", push.MaxRequestBytes, maxPushRequestBytes); err != nil {
		return err
	}
	if err := checkPushIntRange("push_notifications.max_concurrent_requests", push.MaxConcurrentRequests, maxPushConcurrentRequests); err != nil {
		return err
	}
	if err := validatePushProviderList(push.EnabledProviders); err != nil {
		return err
	}
	if err := validatePushAPNs(push); err != nil {
		return err
	}
	return validatePushFCM(push)
}

// validatePushProviderList checks that at least one provider is enabled, that every entry is a known
// provider, and that no entry is repeated.
func validatePushProviderList(names []string) error {
	if len(names) == 0 {
		return errors.New("push_notifications.enabled_providers must name at least one provider when push notifications are enabled")
	}
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if slices.Contains(refusedPushProviders, name) {
			return fmt.Errorf("push_notifications.enabled_providers entry \"%s\" is not available in this build", name)
		}
		if !slices.Contains(knownPushProviders, name) {
			return fmt.Errorf("unknown push_notifications.enabled_providers entry: \"%s\"", name)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("duplicate push_notifications.enabled_providers entry: \"%s\"", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

// validatePushAPNs checks the APNs settings. Only token authentication exists in this build, so the
// certificate settings are refused by the unknown key check and by the auth type check here.
func validatePushAPNs(push configtypes.PushNotifications) error {
	apns := push.APNs
	if !slices.Contains(knownAPNsEndpoints, apns.Endpoint) {
		return fmt.Errorf("unknown push_notifications.apns.endpoint: \"%s\"", apns.Endpoint)
	}
	if apns.AuthType != "token" {
		return fmt.Errorf("push_notifications.apns.auth_type \"%s\" is not available in this build: only token is supported", apns.AuthType)
	}
	if err := checkPushIntRange("push_notifications.apns.tokens_batch_size", apns.TokensBatchSize, maxPushTokensBatchSize); err != nil {
		return err
	}
	if err := checkPushIntRange("push_notifications.apns.max_inflight", apns.MaxInflight, maxPushMaxInflight); err != nil {
		return err
	}
	if apns.TokenKeyFile != "" && apns.TokenKeyPEM.Value() != "" {
		return errors.New("push_notifications.apns.token_key_file and push_notifications.apns.token_key_pem are mutually exclusive")
	}
	if !slices.Contains(push.EnabledProviders, pushProviderAPNs) {
		return nil
	}
	if apns.BundleID == "" {
		return errors.New("push_notifications.apns.bundle_id is required when apns is enabled")
	}
	if apns.TokenKeyID == "" {
		return errors.New("push_notifications.apns.token_key_id is required when apns is enabled")
	}
	if apns.TokenTeamID == "" {
		return errors.New("push_notifications.apns.token_team_id is required when apns is enabled")
	}
	if apns.TokenKeyFile == "" && apns.TokenKeyPEM.Value() == "" {
		return errors.New("push_notifications.apns.token_key_file or push_notifications.apns.token_key_pem is required when apns is enabled")
	}
	return nil
}

// validatePushFCM checks the FCM settings.
func validatePushFCM(push configtypes.PushNotifications) error {
	fcm := push.FCM
	if err := checkPushIntRange("push_notifications.fcm.tokens_batch_size", fcm.TokensBatchSize, maxPushTokensBatchSize); err != nil {
		return err
	}
	if err := checkPushIntRange("push_notifications.fcm.max_inflight", fcm.MaxInflight, maxPushMaxInflight); err != nil {
		return err
	}
	if !slices.Contains(push.EnabledProviders, pushProviderFCM) {
		return nil
	}
	if fcm.CredentialsFile == "" {
		return errors.New("push_notifications.fcm.credentials_file is required when fcm is enabled")
	}
	return nil
}

// checkPushIntRange checks that an integer setting is at least 1 and at most its documented maximum.
func checkPushIntRange(name string, value int, maximum int) error {
	if value < 1 {
		return fmt.Errorf("%s must be greater than zero", name)
	}
	if value > maximum {
		return fmt.Errorf("%s must be at most %d, got %d", name, maximum, value)
	}
	return nil
}

// checkPushDuration checks that a duration setting is positive and at most its documented maximum.
func checkPushDuration(name string, value configtypes.Duration, maximum time.Duration) error {
	if value.ToDuration() <= 0 {
		return fmt.Errorf("%s must be greater than zero", name)
	}
	if value.ToDuration() > maximum {
		return fmt.Errorf("%s must be at most %s, got %s", name, maximum, value.ToDuration())
	}
	return nil
}
