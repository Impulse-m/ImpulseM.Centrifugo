package config

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/config/envconfig"
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

// knownPushProviders are the providers this build sends to.
var knownPushProviders []string = []string{pushProviderAPNs, pushProviderFCM}

// refusedPushProviders are provider names that the configuration model knows of but this build
// does not send to. They are refused at startup rather than accepted and never used.
var refusedPushProviders []string = []string{"hms", "webpush"}

// knownAPNsEndpoints are the APNs hosts.
var knownAPNsEndpoints []string = []string{"production", "development"}

// checkPushUnknownKeys refuses a push notification key that this build does not have, when push
// notifications are enabled. A key is refused both in the configuration file and in the environment
// (CENTRIFUGO_PUSH_NOTIFICATIONS_*, which knownEnvVars does not list). Unknown keys of the rest of
// the configuration keep their warning. The first refused key in sorted order is named.
func checkPushUnknownKeys(conf Config, unknownKeys []string, knownEnvVars map[string]envconfig.VarInfo) error {
	if !conf.PushNotifications.Enabled {
		return nil
	}
	var refused []string
	for _, key := range unknownKeys {
		if strings.HasPrefix(key, pushKeyPrefix) {
			refused = append(refused, key)
		}
	}
	if len(refused) > 0 {
		slices.Sort(refused)
		return fmt.Errorf("%s is not available in this build", refused[0])
	}
	var refusedEnvs []string
	for _, entry := range os.Environ() {
		name, _, found := strings.Cut(entry, "=")
		if !found || !strings.HasPrefix(name, pushEnvPrefix) {
			continue
		}
		if _, ok := knownEnvVars[name]; !ok {
			refusedEnvs = append(refusedEnvs, name)
		}
	}
	if len(refusedEnvs) == 0 {
		return nil
	}
	slices.Sort(refusedEnvs)
	return fmt.Errorf("%s is not available in this build", refusedEnvs[0])
}

// validatePushNotifications checks the push notification settings. Nothing is checked while the
// subsystem is disabled. The values of the settings are checked whenever it is enabled; the settings
// that a provider needs are checked only when that provider is enabled.
func validatePushNotifications(conf Config) error {
	push := conf.PushNotifications
	if !push.Enabled {
		return nil
	}
	if err := checkPositiveDuration("push_notifications.sync_timeout", push.SyncTimeout); err != nil {
		return err
	}
	if err := checkPositiveDuration("push_notifications.providers.request_timeout", push.Providers.RequestTimeout); err != nil {
		return err
	}
	if push.MaxRecipientItems < 1 || push.MaxRecipientItems > maxPushRecipientItems {
		return fmt.Errorf("push_notifications.max_recipient_items must be between 1 and %d, got %d", maxPushRecipientItems, push.MaxRecipientItems)
	}
	if err := checkPositiveInt("push_notifications.max_request_bytes", push.MaxRequestBytes); err != nil {
		return err
	}
	if err := checkPositiveInt("push_notifications.max_concurrent_requests", push.MaxConcurrentRequests); err != nil {
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
	if err := checkPositiveInt("push_notifications.apns.tokens_batch_size", apns.TokensBatchSize); err != nil {
		return err
	}
	if err := checkPositiveInt("push_notifications.apns.max_inflight", apns.MaxInflight); err != nil {
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
	if err := checkPositiveInt("push_notifications.fcm.tokens_batch_size", fcm.TokensBatchSize); err != nil {
		return err
	}
	if err := checkPositiveInt("push_notifications.fcm.max_inflight", fcm.MaxInflight); err != nil {
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

func checkPositiveInt(name string, value int) error {
	if value < 1 {
		return fmt.Errorf("%s must be greater than zero", name)
	}
	return nil
}

func checkPositiveDuration(name string, value configtypes.Duration) error {
	if value.ToDuration() <= 0 {
		return fmt.Errorf("%s must be greater than zero", name)
	}
	return nil
}
