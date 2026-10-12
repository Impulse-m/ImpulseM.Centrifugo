package config

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
)

var knownBrokers = []string{"memory", "nats", "redis", "redisnats"}

// Validate validates config and returns error if problems found.
func (c Config) Validate() error {
	if c.HTTP.H2CExternal && strconv.Itoa(c.HTTP.Port) == c.HTTP.InternalPort {
		return fmt.Errorf("external_h2c requires custom separate internal_port to be configured")
	}
	if c.HTTP.TLS.Enabled && c.HTTP.H2CExternal {
		return fmt.Errorf("external_h2c cannot be used together with enabled TLS for external HTTP server")
	}

	if !slices.Contains(knownBrokers, c.Broker.Type) {
		return fmt.Errorf("unknown broker: %s", c.Broker.Type)
	}

	if err := validateTokens(c); err != nil {
		return err
	}

	if err := validateAccessRevoke(c); err != nil {
		return err
	}

	if c.Client.Proxy.Connect.Enabled {
		if err := validateProxy("default", c.Client.Proxy.Connect.Proxy); err != nil {
			return fmt.Errorf("in client.proxy.connect: %v", err)
		}
	}
	if c.Client.Proxy.Refresh.Enabled {
		if err := validateProxy("default", c.Client.Proxy.Refresh.Proxy); err != nil {
			return fmt.Errorf("in client.proxy.refresh: %v", err)
		}
	}

	if err := validateSecondPrecisionDuration(c.Channel.HistoryMetaTTL); err != nil {
		return fmt.Errorf("in channel.history_meta_ttl: %v", err)
	}

	if !slices.Contains([]string{"", configtypes.PublicationDataFormatJSON, configtypes.PublicationDataFormatJSONObject, configtypes.PublicationDataFormatBinary}, c.Channel.PublicationDataFormat) {
		return fmt.Errorf("unknown channel.publication_data_format: \"%s\"", c.Channel.PublicationDataFormat)
	}

	if err := validateCodeToUniDisconnectTransforms(c.Client.ConnectCodeToUnidirectionalDisconnect.Transforms); err != nil {
		return fmt.Errorf("in client.connect_code_to_unidirectional_disconnect: %v", err)
	}

	var proxyNames []string
	for _, p := range c.Proxies {
		if slices.Contains(proxyNames, p.Name) {
			return fmt.Errorf("duplicate proxy name in channel.named_proxies: %s", p.Name)
		}
		if err := validateProxy(p.Name, p.Proxy); err != nil {
			return fmt.Errorf("in proxy %s: %v", p.Name, err)
		}
		proxyNames = append(proxyNames, p.Name)
	}
	if slices.Contains(proxyNames, DefaultProxyName) {
		return fmt.Errorf("proxy name %s is reserved", DefaultProxyName)
	}
	proxyNames = append(proxyNames, DefaultProxyName) // channel options can use default proxy name.

	if err := validateChannelOptions(c.Channel.WithoutNamespace, c.Channel.HistoryMetaTTL, proxyNames, c); err != nil {
		return fmt.Errorf("in channel.without_namespace: %v", err)
	}

	if err := validateRpcOptions(c.RPC.WithoutNamespace, proxyNames); err != nil {
		return fmt.Errorf("in rpc.without_namespace: %v", err)
	}

	usePersonalChannel := c.Client.SubscribeToUserPersonalChannel.Enabled
	personalChannelNamespace := c.Client.SubscribeToUserPersonalChannel.PersonalChannelNamespace
	personalSingleConnection := c.Client.SubscribeToUserPersonalChannel.SingleConnection
	var validPersonalChannelNamespace bool
	if !usePersonalChannel || personalChannelNamespace == "" {
		validPersonalChannelNamespace = true
		if personalSingleConnection && !c.Channel.WithoutNamespace.Presence {
			return fmt.Errorf("presence must be enabled on top level to maintain single connection")
		}
	}

	nss := make([]string, 0, len(c.Channel.Namespaces))
	for _, n := range c.Channel.Namespaces {
		if slices.Contains(nss, n.Name) {
			return fmt.Errorf("namespace name must be unique: %s", n.Name)
		}
		if err := validateNamespace(n, c.Channel.HistoryMetaTTL, proxyNames, c); err != nil {
			return fmt.Errorf("namespace %s: %v", n.Name, err)
		}
		if n.Name == personalChannelNamespace {
			validPersonalChannelNamespace = true
			if personalSingleConnection && !n.Presence {
				return fmt.Errorf("presence must be enabled for namespace %s to maintain single connection", n.Name)
			}
		}
		nss = append(nss, n.Name)
	}

	if !validPersonalChannelNamespace {
		return fmt.Errorf("namespace for user personal channel not found: %s", personalChannelNamespace)
	}

	rpcNss := make([]string, 0, len(c.RPC.Namespaces))
	for _, n := range c.RPC.Namespaces {
		if slices.Contains(rpcNss, n.Name) {
			return fmt.Errorf("rpc namespace name must be unique: %s", n.Name)
		}
		if err := validateRpcNamespace(n, proxyNames); err != nil {
			return fmt.Errorf("rpc namespace %s: %v", n.Name, err)
		}
		rpcNss = append(rpcNss, n.Name)
	}

	var consumerNames []string
	for _, config := range c.Consumers {
		if !consumerNameRe.Match([]byte(config.Name)) {
			return fmt.Errorf("invalid consumer name: %s, must match %s regular expression", config.Name, consumerNamePattern)
		}
		if slices.Contains(consumerNames, config.Name) {
			return fmt.Errorf("invalid consumer name: %s, must be unique", config.Name)
		}
		if !slices.Contains(configtypes.KnownConsumerTypes, config.Type) {
			return fmt.Errorf("unknown consumer type: %s", config.Type)
		}
		if config.Enabled {
			var err error
			switch config.Type {
			case configtypes.ConsumerTypeKafka:
				err = config.Kafka.Validate()
			case configtypes.ConsumerTypePostgres:
				err = config.Postgres.Validate()
			case configtypes.ConsumerTypeGooglePubSub:
				err = config.GooglePubSub.Validate()
			case configtypes.ConsumerTypeRedisStream:
				err = config.RedisStream.Validate()
			case configtypes.ConsumerTypeNatsJetStream:
				err = config.NatsJetStream.Validate()
			case configtypes.ConsumerTypeAwsSqs:
				err = config.AwsSqs.Validate()
			case configtypes.ConsumerTypeAzureServiceBus:
				err = config.AzureServiceBus.Validate()
			default:
			}
			if err != nil {
				return fmt.Errorf("in consumer %s (%s): %w", config.Name, config.Type, err)
			}
		}
		consumerNames = append(consumerNames, config.Name)
	}

	if err := validateConnectCodeTransforms(c.UniSSE.ConnectCodeToHTTPResponse.Transforms); err != nil {
		return fmt.Errorf("in uni_sse.connect_code_to_http_status.transforms: %v", err)
	}
	if err := validateConnectCodeTransforms(c.UniHTTPStream.ConnectCodeToHTTPResponse.Transforms); err != nil {
		return fmt.Errorf("in uni_http_stream.connect_code_to_http_status.transforms: %v", err)
	}

	if err := validateRateLimit(c); err != nil {
		return fmt.Errorf("in client.rate_limit: %v", err)
	}

	return nil
}

// rateLimitBucketsEntry is a named non-channel operation buckets container.
type rateLimitBucketsEntry struct {
	name    string
	buckets *configtypes.RateLimitBuckets
}

// rateLimitChannelEntry is a named channel operation buckets container.
type rateLimitChannelEntry struct {
	name    string
	buckets *configtypes.RateLimitChannelBuckets
}

// rateLimitSection is a view over a rate limit section used for validation. Fields which
// are not supported by a section are left nil or empty.
type rateLimitSection struct {
	name        string
	total       *configtypes.RateLimitBuckets
	defaults    *configtypes.RateLimitBuckets
	plain       []rateLimitBucketsEntry
	channels    []rateLimitChannelEntry
	unsupported []rateLimitChannelEntry
	rpc         *configtypes.RateLimitRPCBuckets
}

func validateRateLimit(c Config) error {
	nsNames := make([]string, 0, len(c.Channel.Namespaces))
	for _, ns := range c.Channel.Namespaces {
		nsNames = append(nsNames, ns.Name)
	}

	rl := &c.Client.RateLimit
	cc := &rl.ClientCommand
	ccSection := rateLimitSection{
		name:     "client_command",
		total:    &cc.Total,
		defaults: &cc.Default,
		plain:    []rateLimitBucketsEntry{{name: "refresh", buckets: &cc.Refresh}},
		channels: []rateLimitChannelEntry{
			{name: "subscribe", buckets: &cc.Subscribe},
			{name: "unsubscribe", buckets: &cc.Unsubscribe},
			{name: "publish", buckets: &cc.Publish},
			{name: "history", buckets: &cc.History},
			{name: "presence", buckets: &cc.Presence},
			{name: "presence_stats", buckets: &cc.PresenceStats},
			{name: "sub_refresh", buckets: &cc.SubRefresh},
		},
		unsupported: []rateLimitChannelEntry{
			{name: "map_publish", buckets: &cc.MapPublish},
			{name: "map_remove", buckets: &cc.MapRemove},
			{name: "track", buckets: &cc.Track},
			{name: "untrack", buckets: &cc.Untrack},
		},
		rpc: &cc.RPC,
	}
	if err := validateRateLimitSection(ccSection, nsNames); err != nil {
		return err
	}

	uc := &rl.UserCommand
	ucSection := rateLimitSection{
		name:     "user_command",
		total:    &uc.Total,
		defaults: &uc.Default,
		plain: []rateLimitBucketsEntry{
			{name: "connect", buckets: &uc.Connect},
			{name: "refresh", buckets: &uc.Refresh},
		},
		channels: []rateLimitChannelEntry{
			{name: "subscribe", buckets: &uc.Subscribe},
			{name: "unsubscribe", buckets: &uc.Unsubscribe},
			{name: "publish", buckets: &uc.Publish},
			{name: "history", buckets: &uc.History},
			{name: "presence", buckets: &uc.Presence},
			{name: "presence_stats", buckets: &uc.PresenceStats},
			{name: "sub_refresh", buckets: &uc.SubRefresh},
		},
		unsupported: []rateLimitChannelEntry{
			{name: "map_publish", buckets: &uc.MapPublish},
			{name: "map_remove", buckets: &uc.MapRemove},
			{name: "track", buckets: &uc.Track},
			{name: "untrack", buckets: &uc.Untrack},
		},
		rpc: &uc.RPC,
	}
	if err := validateRateLimitSection(ucSection, nsNames); err != nil {
		return err
	}

	rc := &rl.RedisUserCommand
	rcSection := rateLimitSection{
		name:     "redis_user_command",
		total:    &rc.Total,
		defaults: &rc.Default,
		plain: []rateLimitBucketsEntry{
			{name: "connect", buckets: &rc.Connect},
			{name: "refresh", buckets: &rc.Refresh},
		},
		channels: []rateLimitChannelEntry{
			{name: "subscribe", buckets: &rc.Subscribe},
			{name: "unsubscribe", buckets: &rc.Unsubscribe},
			{name: "publish", buckets: &rc.Publish},
			{name: "history", buckets: &rc.History},
			{name: "presence", buckets: &rc.Presence},
			{name: "presence_stats", buckets: &rc.PresenceStats},
			{name: "sub_refresh", buckets: &rc.SubRefresh},
		},
		unsupported: []rateLimitChannelEntry{
			{name: "map_publish", buckets: &rc.MapPublish},
			{name: "map_remove", buckets: &rc.MapRemove},
			{name: "track", buckets: &rc.Track},
			{name: "untrack", buckets: &rc.Untrack},
		},
		rpc: &rc.RPC,
	}
	if err := validateRateLimitSection(rcSection, nsNames); err != nil {
		return err
	}
	if rc.Total.Enabled || len(rc.Total.Buckets) > 0 {
		return fmt.Errorf("redis_user_command.total is not supported, use per-operation buckets instead")
	}
	if rc.Redis.ReuseFromEngine && rc.Enabled && c.Engine.Type != "redis" {
		return fmt.Errorf("redis_user_command.redis.reuse_from_engine requires engine.type to be redis")
	}

	if err := validateRateLimitSection(rateLimitSection{
		name:  "client_error",
		total: &rl.ClientError.Total,
	}, nsNames); err != nil {
		return err
	}

	if uc.Enabled {
		return fmt.Errorf("user_command: not implemented yet")
	}
	if rc.Enabled {
		return fmt.Errorf("redis_user_command: not implemented yet")
	}
	return nil
}

func validateRateLimitSection(s rateLimitSection, nsNames []string) error {
	if s.total != nil {
		if err := validateRateLimitBuckets(s.total.Enabled, s.total.Buckets, true); err != nil {
			return fmt.Errorf("%s.total: %v", s.name, err)
		}
	}
	if s.defaults != nil {
		if err := validateRateLimitBuckets(s.defaults.Enabled, s.defaults.Buckets, true); err != nil {
			return fmt.Errorf("%s.default: %v", s.name, err)
		}
	}
	for _, entry := range s.plain {
		if err := validateRateLimitBuckets(entry.buckets.Enabled, entry.buckets.Buckets, true); err != nil {
			return fmt.Errorf("%s.%s: %v", s.name, entry.name, err)
		}
	}
	for _, entry := range s.channels {
		if err := validateRateLimitBuckets(entry.buckets.Enabled, entry.buckets.Buckets, true); err != nil {
			return fmt.Errorf("%s.%s: %v", s.name, entry.name, err)
		}
		if err := validateRateLimitNamespaceOverrides(entry.buckets.NamespaceOverrides, nsNames); err != nil {
			return fmt.Errorf("%s.%s: %v", s.name, entry.name, err)
		}
	}
	for _, entry := range s.unsupported {
		if entry.buckets.Enabled || len(entry.buckets.Buckets) > 0 || len(entry.buckets.NamespaceOverrides) > 0 {
			return fmt.Errorf("%s.%s: not supported in this build", s.name, entry.name)
		}
	}
	if s.rpc != nil {
		if err := validateRateLimitBuckets(s.rpc.Enabled, s.rpc.Buckets, true); err != nil {
			return fmt.Errorf("%s.rpc: %v", s.name, err)
		}
		if len(s.rpc.MethodOverride) > 0 && len(s.rpc.MethodOverrides) > 0 {
			return fmt.Errorf("%s.rpc: method_override and method_overrides cannot be used together", s.name)
		}
		if err := validateRateLimitMethodOverrides(s.rpc.MethodOverrides); err != nil {
			return fmt.Errorf("%s.rpc: %v", s.name, err)
		}
		for method, override := range s.rpc.MethodOverride {
			if err := validateRateLimitBuckets(override.Enabled, override.Buckets, false); err != nil {
				return fmt.Errorf("%s.rpc.method_override.%s: %v", s.name, method, err)
			}
		}
	}
	return nil
}

// validateRateLimitBuckets validates bucket values of an enabled container. An enabled container
// without buckets is an error when requireBuckets is set.
func validateRateLimitBuckets(enabled bool, buckets configtypes.RateLimitBucketList, requireBuckets bool) error {
	if !enabled {
		return nil
	}
	if requireBuckets && len(buckets) == 0 {
		return fmt.Errorf("enabled buckets must not be empty")
	}
	for _, bucket := range buckets {
		if bucket.Interval.ToDuration() <= 0 {
			return fmt.Errorf("bucket interval must be greater than 0")
		}
		if bucket.Rate <= 0 {
			return fmt.Errorf("bucket rate must be greater than 0")
		}
		if bucket.Interval.ToDuration() < time.Duration(bucket.Rate) {
			return fmt.Errorf("bucket interval %s is too small for rate %d", bucket.Interval.ToDuration(), bucket.Rate)
		}
	}
	return nil
}

func validateRateLimitNamespaceOverrides(overrides configtypes.RateLimitNamespaceOverrides, nsNames []string) error {
	seen := make([]string, 0, len(overrides))
	for _, override := range overrides {
		if slices.Contains(seen, override.NamespaceName) {
			return fmt.Errorf("duplicate namespace_name in namespace_overrides: %s", override.NamespaceName)
		}
		if !slices.Contains(nsNames, override.NamespaceName) {
			return fmt.Errorf("namespace_name %s in namespace_overrides not found in channel.namespaces", override.NamespaceName)
		}
		if err := validateRateLimitBuckets(override.Enabled, override.Buckets, false); err != nil {
			return fmt.Errorf("namespace override %s: %v", override.NamespaceName, err)
		}
		seen = append(seen, override.NamespaceName)
	}
	return nil
}

func validateRateLimitMethodOverrides(overrides configtypes.RateLimitMethodOverrides) error {
	seen := make([]string, 0, len(overrides))
	for _, override := range overrides {
		if slices.Contains(seen, override.Method) {
			return fmt.Errorf("duplicate method in method_overrides: %s", override.Method)
		}
		if err := validateRateLimitBuckets(override.Enabled, override.Buckets, false); err != nil {
			return fmt.Errorf("method override %s: %v", override.Method, err)
		}
		seen = append(seen, override.Method)
	}
	return nil
}

var namePattern = "^[-a-zA-Z0-9_.]{2,}$"
var nameRe = regexp.MustCompile(namePattern)

func validateNamespace(ns configtypes.ChannelNamespace, globalHistoryMetaTTL configtypes.Duration, proxyNames []string, cfg Config) error {
	name := ns.Name
	match := nameRe.MatchString(name)
	if !match {
		return fmt.Errorf("invalid namespace name – %s (must match %s regular expression)", name, namePattern)
	}
	if err := validateChannelOptions(ns.ChannelOptions, globalHistoryMetaTTL, proxyNames, cfg); err != nil {
		return err
	}
	return nil
}

func validateRpcNamespace(ns configtypes.RpcNamespace, rpcProxyNames []string) error {
	name := ns.Name
	match := nameRe.MatchString(name)
	if !match {
		return fmt.Errorf("invalid rpc namespace name – %s (must match %s regular expression)", name, namePattern)
	}
	if err := validateRpcOptions(ns.RpcOptions, rpcProxyNames); err != nil {
		return err
	}
	return nil
}

func validateChannelOptions(c configtypes.ChannelOptions, globalHistoryMetaTTL configtypes.Duration, proxyNames []string, cfg Config) error {
	if err := validateSecondPrecisionDuration(c.HistoryTTL); err != nil {
		return fmt.Errorf("in history_ttl: %v", err)
	}
	if err := validateSecondPrecisionDuration(c.HistoryMetaTTL); err != nil {
		return fmt.Errorf("in history_meta_ttl: %v", err)
	}
	if (c.HistorySize != 0 && c.HistoryTTL == 0) || (c.HistorySize == 0 && c.HistoryTTL != 0) {
		return errors.New("both history size and history ttl required for history")
	}
	historyMetaTTL := globalHistoryMetaTTL
	if c.HistoryMetaTTL != 0 {
		historyMetaTTL = c.HistoryMetaTTL
	}
	if historyMetaTTL < c.HistoryTTL {
		return fmt.Errorf("history ttl (%s) can not be greater than history meta ttl (%s)", c.HistoryTTL, historyMetaTTL)
	}
	if c.ForceRecovery && (c.HistorySize == 0 || c.HistoryTTL == 0) {
		return errors.New("both history size and history ttl required for recovery")
	}
	if c.SharedPositionSync && !c.ForcePositioning && !c.AllowPositioning && !c.ForceRecovery && !c.AllowRecovery {
		return errors.New("shared_position_sync requires positioning or recovery")
	}
	if c.SharedPositionSync && cfg.Client.ChannelPositionCheckDelay == 0 {
		return errors.New("shared_position_sync requires client.channel_position_check_delay to be greater than zero")
	}
	if c.ChannelRegex != "" {
		if _, err := regexp.Compile(c.ChannelRegex); err != nil {
			return fmt.Errorf("invalid channel regex %s: %w", c.ChannelRegex, err)
		}
	}
	if (c.SubscribeStreamProxyEnabled) && (c.SubscribeProxyEnabled || c.PublishProxyEnabled || c.SubRefreshProxyEnabled) {
		return fmt.Errorf("can't use subscribe stream proxy together with subscribe, publish or sub refresh proxies")
	}
	if len(c.AllowedDeltaTypes) > 0 {
		for _, dt := range c.AllowedDeltaTypes {
			if !slices.Contains([]centrifuge.DeltaType{centrifuge.DeltaTypeFossil}, dt) {
				return fmt.Errorf("unknown allowed delta type: \"%s\"", dt)
			}
		}
	}
	if !slices.Contains([]string{"", "stream", "cache"}, c.ForceRecoveryMode) {
		return fmt.Errorf("unknown recovery mode: \"%s\"", c.ForceRecoveryMode)
	}

	if !slices.Contains([]string{"", configtypes.PublicationDataFormatJSON, configtypes.PublicationDataFormatJSONObject, configtypes.PublicationDataFormatBinary}, c.PublicationDataFormat) {
		return fmt.Errorf("unknown publication_data_format: \"%s\"", c.PublicationDataFormat)
	}

	if c.SubscribeProxyName != "" && !slices.Contains(proxyNames, c.SubscribeProxyName) {
		return fmt.Errorf("subscribe proxy with name \"%s\" not found", c.SubscribeProxyName)
	}
	if c.SubscribeProxyEnabled && c.SubscribeProxyName == DefaultProxyName {
		if err := validateProxy("default", cfg.Channel.Proxy.Subscribe); err != nil {
			return fmt.Errorf("in channel.proxy.subscribe: %v", err)
		}
	}

	if c.PublishProxyName != "" && !slices.Contains(proxyNames, c.PublishProxyName) {
		return fmt.Errorf("publish proxy with name \"%s\" not found", c.PublishProxyName)
	}
	if c.PublishProxyEnabled && c.PublishProxyName == DefaultProxyName {
		if err := validateProxy("default", cfg.Channel.Proxy.Publish); err != nil {
			return fmt.Errorf("in channel.proxy.publish: %v", err)
		}
	}

	if c.SubRefreshProxyName != "" && !slices.Contains(proxyNames, c.SubRefreshProxyName) {
		return fmt.Errorf("sub refresh proxy with name \"%s\" not found", c.SubRefreshProxyName)
	}
	if c.SubRefreshProxyEnabled && c.SubRefreshProxyName == DefaultProxyName {
		if err := validateProxy("default", cfg.Channel.Proxy.SubRefresh); err != nil {
			return fmt.Errorf("in channel.proxy.sub_refresh: %v", err)
		}
	}

	if c.SubscribeStreamProxyName != "" && !slices.Contains(proxyNames, c.SubscribeStreamProxyName) {
		return fmt.Errorf("subscribe stream proxy with name \"%s\" not found", c.SubscribeStreamProxyName)
	}
	if c.SubscribeStreamProxyEnabled && c.SubscribeStreamProxyName == DefaultProxyName {
		if err := validateProxy("default", cfg.Channel.Proxy.SubscribeStream); err != nil {
			return fmt.Errorf("in channel.proxy.subscribe_stream: %v", err)
		}
	}

	return nil
}

func validateRpcOptions(opts configtypes.RpcOptions, rpcProxyNames []string) error {
	if opts.ProxyName != "" && !slices.Contains(rpcProxyNames, opts.ProxyName) {
		return fmt.Errorf("proxy %s not found for rpc", opts.ProxyName)
	}
	return nil
}

var proxyNamePattern = "^[-a-zA-Z0-9_.]{2,}$"
var proxyNameRe = regexp.MustCompile(proxyNamePattern)

var consumerNamePattern = "^[a-zA-Z0-9_]{2,}$"
var consumerNameRe = regexp.MustCompile(consumerNamePattern)

func validateSecondPrecisionDuration(duration configtypes.Duration) error {
	if duration > 0 && duration.ToDuration()%time.Second != 0 {
		return fmt.Errorf("malformed duration %s, sub-second precision is not supported for this key", duration)
	}
	return nil
}

func validateProxy(name string, p configtypes.Proxy) error {
	if !proxyNameRe.MatchString(name) {
		return fmt.Errorf("invalid proxy name: %s, must match %s regular expression", name, proxyNamePattern)
	}
	if p.Timeout == 0 {
		return errors.New("timeout not set")
	}
	if p.Endpoint == "" {
		return errors.New("endpoint not set")
	}
	if err := validateStatusTransforms(p.ProxyCommon.HTTP.StatusToCodeTransforms); err != nil {
		return fmt.Errorf("in status_to_code_transforms: %v", err)
	}
	return nil
}

// Now Centrifugo uses https://github.com/tidwall/gjson to extract custom claims from JWT. So technically
// we could support extracting from nested objects using dot syntax, like "centrifugo.user". But for now
// not using this feature to keep things simple until necessary.
var customClaimRe = regexp.MustCompile("^[a-zA-Z_]+$")

func validateTokens(cfg Config) error {
	if cfg.Client.Token.UserIDClaim != "" {
		if !customClaimRe.MatchString(cfg.Client.Token.UserIDClaim) {
			return fmt.Errorf("invalid token custom user ID claim: %s, must match %s regular expression", cfg.Client.Token.UserIDClaim, customClaimRe.String())
		}
	}
	if cfg.Client.SubscriptionToken.UserIDClaim != "" {
		if !customClaimRe.MatchString(cfg.Client.SubscriptionToken.UserIDClaim) {
			return fmt.Errorf("invalid subscription token custom user ID claim: %s, must match %s regular expression", cfg.Client.SubscriptionToken.UserIDClaim, customClaimRe.String())
		}
	}
	if err := validateEd25519PublicKeys("client.token", cfg.Client.Token); err != nil {
		return err
	}
	if err := validateEd25519PublicKeys("client.subscription_token.token", cfg.Client.SubscriptionToken.Token); err != nil {
		return err
	}
	return nil
}

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
	}
	return nil
}

func validateStatusTransforms(transforms []configtypes.HttpStatusToCodeTransform) error {
	for i, transform := range transforms {
		if transform.StatusCode == 0 {
			return fmt.Errorf("status_code should be set in status_to_code_transforms[%d]", i)
		}
		if transform.ToDisconnect.Code == 0 && transform.ToError.Code == 0 {
			return fmt.Errorf("no error or disconnect code set in status_to_code_transforms[%d]", i)
		}
		if transform.ToDisconnect.Code > 0 && transform.ToError.Code > 0 {
			return fmt.Errorf("only error or disconnect code can be set in status_to_code_transforms[%d], but not both", i)
		}
		if !tools.IsASCII(transform.ToDisconnect.Reason) {
			return fmt.Errorf("status_to_code_transforms[%d] disconnect reason must be ASCII", i)
		}
		if !tools.IsASCII(transform.ToError.Message) {
			return fmt.Errorf("status_to_code_transforms[%d] error message must be ASCII", i)
		}
		const reasonOrMessageMaxLength = 123 // limit comes from WebSocket close reason length limit. See https://datatracker.ietf.org/doc/html/rfc6455.
		if len(transform.ToError.Message) > reasonOrMessageMaxLength {
			return fmt.Errorf("status_to_code_transforms[%d] item error message can be up to %d characters long", i, reasonOrMessageMaxLength)
		}
		if len(transform.ToDisconnect.Reason) > reasonOrMessageMaxLength {
			return fmt.Errorf("status_to_code_transforms[%d] disconnect reason can be up to %d characters long", i, reasonOrMessageMaxLength)
		}
	}
	return nil
}

func validateConnectCodeTransforms(transforms []configtypes.ConnectCodeToHTTPResponseTransform) error {
	for i, transform := range transforms {
		if transform.Code == 0 {
			return fmt.Errorf("code should be set in connect_code_to_http_status.transforms[%d]", i)
		}
		if transform.To.StatusCode == 0 {
			return fmt.Errorf("status_code should be set in connect_code_to_http_status.transforms[%d].to_response", i)
		}
	}
	return nil
}

func validateCodeToUniDisconnectTransforms(transforms configtypes.UniConnectCodeToDisconnectTransforms) error {
	for i, t := range transforms {
		if t.Code == 0 {
			return fmt.Errorf("no code specified in transforms[%d]", i)
		}
		if t.To.Code == 0 {
			return errors.New("no disconnect code specified in transforms[%d].to")
		}
		if !tools.IsASCII(t.To.Reason) {
			return errors.New("disconnect reason must be ASCII in transforms[%d].to")
		}
	}
	return nil
}
