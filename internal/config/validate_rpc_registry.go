package config

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/redisshard"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"
	"github.com/rs/zerolog/log"

	"google.golang.org/grpc/codes"
)

// rpcStatusToCodeTransformsKey is the config key of the gRPC status transforms, used in messages.
const rpcStatusToCodeTransformsKey = "grpc_status_to_code_transforms"

var (
	rpcRegistryMethodRe     = regexp.MustCompile(`^[A-Za-z0-9_.:/-]{1,128}$`)
	rpcRegistryGRPCMethodRe = regexp.MustCompile(`^/[\w.]+/\w+$`)
)

// validateRPCRegistry validates the RPC method registry. A disabled registry is not checked. An enabled one
// is checked in full, including the registry file, so checkconfig covers it.
func validateRPCRegistry(c Config) error {
	err := validateRPCRegistryTransforms(c.RPC.Registry.GRPCStatusToCodeTransforms)
	if err != nil {
		return fmt.Errorf("in rpc.registry: %w", err)
	}
	err = validateRPCDedup(c)
	if err != nil {
		return fmt.Errorf("in rpc.registry: %w", err)
	}
	if !c.RPC.Registry.Enabled {
		return nil
	}
	if c.RPC.Registry.PollInterval.ToDuration() < 0 {
		return fmt.Errorf("rpc.registry.poll_interval: must not be negative, got %s", c.RPC.Registry.PollInterval.ToDuration())
	}
	if c.RPC.Registry.PendingMaxLifetime.ToDuration() < 0 {
		return fmt.Errorf("rpc.registry.pending_2fa_max_lifetime: must not be negative, got %s", c.RPC.Registry.PendingMaxLifetime.ToDuration())
	}
	file, err := ResolveRPCRegistry(c.RPC.Registry)
	if err != nil {
		return err
	}
	for _, warning := range rpcRegistryLimitWarnings(c, file) {
		log.Warn().Msg(warning)
	}
	return nil
}

// validateRPCDedup checks the exactly-once RPC config. A disabled dedup is not checked. Dedup needs the
// enabled registry, and the server time in replies, which the SDK uses to correct its msg_id clock.
func validateRPCDedup(c Config) error {
	dedup := c.RPC.Registry.Dedup
	if !dedup.Enabled {
		return nil
	}
	if !c.RPC.Registry.Enabled {
		return errors.New("rpc.registry.dedup.enabled: requires rpc.registry.enabled")
	}
	if !c.Client.ReplyIncludeServerTime {
		return errors.New("rpc.registry.dedup.enabled: requires client.reply_include_server_time")
	}
	if dedup.Window.ToDuration() < time.Second {
		return fmt.Errorf("rpc.registry.dedup.window: must be at least 1s, got %s", dedup.Window.ToDuration())
	}
	if dedup.MaxSkew.ToDuration() < 0 {
		return fmt.Errorf("rpc.registry.dedup.max_skew: must not be negative, got %s", dedup.MaxSkew.ToDuration())
	}
	if dedup.Window.ToDuration() <= dedup.MaxSkew.ToDuration() {
		return fmt.Errorf("rpc.registry.dedup.window: must be greater than max_skew, got window %s and max_skew %s", dedup.Window.ToDuration(), dedup.MaxSkew.ToDuration())
	}
	if dedup.MaxReplySize <= 0 {
		return fmt.Errorf("rpc.registry.dedup.max_reply_size: must be positive, got %d", dedup.MaxReplySize)
	}
	if err := validateRPCDedupBudgets(dedup); err != nil {
		return err
	}
	// A cancel stores the cancelled error as an answer in both tiers, which count an error by its encoded size and keep
	// an answer up to the reply limit. The encoded error must fit that limit, so a tombstone always fits both tiers.
	cancelled := c.RPC.Registry.Errors.Cancelled
	cancelledError := &centrifuge.Error{
		Code:    cancelled.Code,
		Message: cancelled.Message,
		AppCode: cancelled.AppCode,
		AppType: cancelled.AppType,
	}
	if limit, size := dedup.ReplyLimit(), rpcdedup.ErrorSize(cancelledError); size > limit {
		return fmt.Errorf("rpc.registry.errors.cancelled.message: the encoded cancelled error must fit the reply limit of %d bytes, got %d bytes", limit, size)
	}
	if !slices.Contains(configtypes.RPCDedupStoreErrors, dedup.OnStoreError) {
		return fmt.Errorf("rpc.registry.dedup.on_store_error: must be one of %s, got %q", strings.Join(configtypes.RPCDedupStoreErrors, ", "), dedup.OnStoreError)
	}
	if dedup.Redis.Enabled {
		return validateRPCDedupRedis(c, dedup.Redis)
	}
	return nil
}

// The bounds of the waiter nodes of a record, see rpc.registry.dedup.redis.wakeup.max_waiter_nodes.
const (
	minRPCDedupMaxWaiterNodes = 1
	maxRPCDedupMaxWaiterNodes = 1024
)

// validateRPCDedupRedis checks the shared tier of exactly-once RPC: either it reuses the Redis of the engine, or its own
// Redis configuration is valid and has a prefix. The wakeup checks its waiter cap only when it is on.
func validateRPCDedupRedis(c Config, redis configtypes.RPCDedupRedis) error {
	if redis.Wakeup.Enabled && !isRPCDedupMaxWaiterNodesInRange(redis.Wakeup.MaxWaiterNodes) {
		return fmt.Errorf("rpc.registry.dedup.redis.wakeup.max_waiter_nodes: must be between %d and %d, got %d", minRPCDedupMaxWaiterNodes, maxRPCDedupMaxWaiterNodes, redis.Wakeup.MaxWaiterNodes)
	}
	if redis.ReuseFromEngine {
		if c.Engine.Type != "redis" {
			return errors.New("rpc.registry.dedup.redis.reuse_from_engine requires engine.type to be redis")
		}
		return nil
	}
	if _, err := redisshard.ConfigsOf(redis.Redis, "rpc.registry.dedup.redis"); err != nil {
		return err
	}
	if redis.Prefix == "" {
		return errors.New("rpc.registry.dedup.redis.prefix: must not be empty")
	}
	return nil
}

// validateRPCDedupBudgets checks the budgets of the dedup store and the reserves of its scopes. The authenticated
// and the anonymous pools each need one scope per shard, so that a shard can admit a scope at all, and the reserve
// bytes must fit the byte budget of the pool, so that the store keeps its reserves. Each record of a scope is kept up
// to its share of the reserve bytes, so the share must hold a record which is kept as uncached.
func validateRPCDedupBudgets(dedup configtypes.RPCDedup) error {
	if dedup.LocalMaxEntries <= 0 {
		return fmt.Errorf("rpc.registry.dedup.local_max_entries: must be positive, got %d", dedup.LocalMaxEntries)
	}
	if dedup.LocalMaxBytes <= 0 {
		return fmt.Errorf("rpc.registry.dedup.local_max_bytes: must be positive, got %d", dedup.LocalMaxBytes)
	}
	if dedup.MaxRecordsPerScope <= 0 {
		return fmt.Errorf("rpc.registry.dedup.max_records_per_scope: must be positive, got %d", dedup.MaxRecordsPerScope)
	}
	if dedup.ScopeReserve < 1 {
		return fmt.Errorf("rpc.registry.dedup.scope_reserve: must be at least 1, got %d", dedup.ScopeReserve)
	}
	if dedup.ScopeReserveBytes < 1024 {
		return fmt.Errorf("rpc.registry.dedup.scope_reserve_bytes: must be at least 1024, got %d", dedup.ScopeReserveBytes)
	}
	if dedup.ScopeReserveBytes < dedup.ScopeReserve*rpcdedup.RecordOverhead {
		return fmt.Errorf("rpc.registry.dedup.scope_reserve_bytes: must be at least scope_reserve %d times %d bytes per record, got %d", dedup.ScopeReserve, rpcdedup.RecordOverhead, dedup.ScopeReserveBytes)
	}
	if dedup.MaxScopesPerUser < 1 {
		return fmt.Errorf("rpc.registry.dedup.max_scopes_per_user: must be at least 1, got %d", dedup.MaxScopesPerUser)
	}
	if dedup.MaxRecordsPerScope < dedup.ScopeReserve {
		return fmt.Errorf("rpc.registry.dedup.max_records_per_scope: must be at least scope_reserve %d, got %d", dedup.ScopeReserve, dedup.MaxRecordsPerScope)
	}
	if dedup.LocalAnonymousMaxEntries < 0 || dedup.LocalAnonymousMaxEntries >= dedup.LocalMaxEntries {
		return fmt.Errorf("rpc.registry.dedup.local_anonymous_max_entries: must be at least 0 and less than local_max_entries %d, got %d", dedup.LocalMaxEntries, dedup.LocalAnonymousMaxEntries)
	}
	if dedup.LocalAnonymousMaxBytes < 0 || dedup.LocalAnonymousMaxBytes >= dedup.LocalMaxBytes {
		return fmt.Errorf("rpc.registry.dedup.local_anonymous_max_bytes: must be at least 0 and less than local_max_bytes %d, got %d", dedup.LocalMaxBytes, dedup.LocalAnonymousMaxBytes)
	}
	authEntries := dedup.LocalMaxEntries - dedup.LocalAnonymousMaxEntries
	if authEntries < rpcdedup.DefaultShards*dedup.ScopeReserve {
		return fmt.Errorf("rpc.registry.dedup.local_max_entries: local_max_entries minus local_anonymous_max_entries must be at least %d, one scope per shard, got %d", rpcdedup.DefaultShards*dedup.ScopeReserve, authEntries)
	}
	authBytes := dedup.LocalMaxBytes - dedup.LocalAnonymousMaxBytes
	if authBytes < rpcdedup.DefaultShards*dedup.ScopeReserveBytes {
		return fmt.Errorf("rpc.registry.dedup.local_max_bytes: local_max_bytes minus local_anonymous_max_bytes must be at least %d, one scope per shard, got %d", rpcdedup.DefaultShards*dedup.ScopeReserveBytes, authBytes)
	}
	if dedup.LocalAnonymousMaxEntries == 0 {
		if dedup.LocalAnonymousMaxBytes != 0 {
			return fmt.Errorf("rpc.registry.dedup.local_anonymous_max_bytes: must be 0 when local_anonymous_max_entries is 0, got %d", dedup.LocalAnonymousMaxBytes)
		}
		return nil
	}
	if dedup.LocalAnonymousMaxEntries < rpcdedup.DefaultShards*dedup.ScopeReserve {
		return fmt.Errorf("rpc.registry.dedup.local_anonymous_max_entries: must be 0, or at least %d, one scope per shard, got %d", rpcdedup.DefaultShards*dedup.ScopeReserve, dedup.LocalAnonymousMaxEntries)
	}
	if dedup.LocalAnonymousMaxBytes < rpcdedup.DefaultShards*dedup.ScopeReserveBytes {
		return fmt.Errorf("rpc.registry.dedup.local_anonymous_max_bytes: must be at least %d, one scope per shard, got %d", rpcdedup.DefaultShards*dedup.ScopeReserveBytes, dedup.LocalAnonymousMaxBytes)
	}
	return nil
}

// validateRPCAuthClassClaim checks auth_class_claim against the entries of the registry. Methods of class
// pending_2fa or bot can only be allowed when the claim which carries the class is configured.
func validateRPCAuthClassClaim(claim string, file configtypes.RPCRegistryFile) error {
	if claim != "" {
		if !customClaimRe.MatchString(claim) {
			return fmt.Errorf("rpc.registry.auth_class_claim: invalid claim %s, must match %s regular expression", claim, customClaimRe.String())
		}
		return nil
	}
	if authClassNeedsClaim(file.Defaults.AuthClass) {
		return fmt.Errorf("rpc registry: defaults: auth_class: %q requires rpc.registry.auth_class_claim to be set", file.Defaults.AuthClass)
	}
	for _, method := range file.Methods {
		authClass := method.AuthClass
		if authClass == "" {
			authClass = file.Defaults.AuthClass
		}
		if authClassNeedsClaim(authClass) {
			return fmt.Errorf("rpc registry method %q: auth_class: %q requires rpc.registry.auth_class_claim to be set", method.Method, authClass)
		}
	}
	return nil
}

// authClassNeedsClaim reports whether an auth class can only be granted by the claim configured in
// rpc.registry.auth_class_claim.
func authClassNeedsClaim(authClass string) bool {
	return authClass == configtypes.RPCAuthClassPending2FA || authClass == configtypes.RPCAuthClassBot
}

// validateRPCRegistryFile checks the entries of a registry file, whether read from a file or given inline.
// Every error names the method or target and the field.
func validateRPCRegistryFile(file configtypes.RPCRegistryFile) error {
	if file.Version != 1 {
		return fmt.Errorf("rpc registry: version must be 1, got %d", file.Version)
	}
	err := validateRPCMethodDefaults(file.Defaults)
	if err != nil {
		return fmt.Errorf("rpc registry: defaults: %w", err)
	}

	targetNames := make(map[string]struct{}, len(file.Targets))
	for i, target := range file.Targets {
		label := fmt.Sprintf("targets[%d]", i)
		if target.Name != "" {
			label = fmt.Sprintf("target %q", target.Name)
		}
		err := validateRPCTarget(target)
		if err != nil {
			return fmt.Errorf("rpc registry %s: %w", label, err)
		}
		_, exists := targetNames[target.Name]
		if exists {
			return fmt.Errorf("rpc registry %s: name: duplicate target name", label)
		}
		targetNames[target.Name] = struct{}{}
	}

	methodNames := make(map[string]struct{}, len(file.Methods))
	for i, method := range file.Methods {
		label := fmt.Sprintf("methods[%d]", i)
		if method.Method != "" {
			label = fmt.Sprintf("method %q", method.Method)
		}
		err := validateRPCMethod(method, file.Defaults, targetNames)
		if err != nil {
			return fmt.Errorf("rpc registry %s: %w", label, err)
		}
		_, exists := methodNames[method.Method]
		if exists {
			return fmt.Errorf("rpc registry %s: method: duplicate method name", label)
		}
		methodNames[method.Method] = struct{}{}
	}
	return nil
}

func validateRPCMethodDefaults(defaults configtypes.RPCMethodDefaults) error {
	if defaults.AuthClass != "" && !slices.Contains(configtypes.RPCAuthClasses, defaults.AuthClass) {
		return fmt.Errorf("auth_class: unknown auth class %q", defaults.AuthClass)
	}
	return validateRPCLimits(defaults.Timeout, defaults.MaxRequestBytes, defaults.MaxResponseBytes)
}

func validateRPCTarget(target configtypes.RPCTarget) error {
	if target.Name == "" {
		return fmt.Errorf("name: must be set")
	}
	if target.Endpoint == "" {
		return fmt.Errorf("endpoint: must be set")
	}
	if target.LoadBalancing != "" && !slices.Contains(configtypes.RPCLoadBalancingPolicies, target.LoadBalancing) {
		return fmt.Errorf("load_balancing: unknown policy %q", target.LoadBalancing)
	}
	err := validateRPCLimits(target.Timeout, target.MaxRequestBytes, target.MaxResponseBytes)
	if err != nil {
		return err
	}
	return validateRPCHeaderNames(target.EmulatedHeaders, "emulated_headers")
}

// validateRPCMethod checks one method against the defaults and the target names. A method
// may leave auth_class empty only when the defaults give a class other than anonymous.
func validateRPCMethod(method configtypes.RPCMethod, defaults configtypes.RPCMethodDefaults, targetNames map[string]struct{}) error {
	if !rpcRegistryMethodRe.MatchString(method.Method) {
		return fmt.Errorf("method: must match %s", rpcRegistryMethodRe.String())
	}
	if !rpcRegistryGRPCMethodRe.MatchString(method.GRPCMethod) {
		return fmt.Errorf("grpc_method: must match %s, got %q", rpcRegistryGRPCMethodRe.String(), method.GRPCMethod)
	}
	_, targetExists := targetNames[method.Target]
	if !targetExists {
		return fmt.Errorf("target: unknown target %q", method.Target)
	}
	if method.AuthClass == "" {
		if defaults.AuthClass == "" || defaults.AuthClass == configtypes.RPCAuthClassAnonymous {
			return fmt.Errorf("auth_class: must be set, defaults.auth_class is empty or anonymous")
		}
	} else if !slices.Contains(configtypes.RPCAuthClasses, method.AuthClass) {
		return fmt.Errorf("auth_class: unknown auth class %q", method.AuthClass)
	}
	err := validateRPCLimits(method.Timeout, method.MaxRequestBytes, method.MaxResponseBytes)
	if err != nil {
		return err
	}
	err = validateRPCHeaderNames(method.Headers, "headers")
	if err != nil {
		return err
	}
	return validateRPCLowerCaseNames(method.ErrorMetadata, "error_metadata")
}

// validateRPCLimits rejects negative timeouts and sizes. Zero is allowed and means the value is inherited.
func validateRPCLimits(timeout configtypes.Duration, maxRequestBytes int, maxResponseBytes int) error {
	if timeout < 0 {
		return fmt.Errorf("timeout: must not be negative")
	}
	if maxRequestBytes < 0 {
		return fmt.Errorf("max_request_bytes: must not be negative")
	}
	if maxResponseBytes < 0 {
		return fmt.Errorf("max_response_bytes: must not be negative")
	}
	return nil
}

// validateRPCHeaderNames checks a header allow-list: names are lower-case and none is reserved.
func validateRPCHeaderNames(names []string, field string) error {
	err := validateRPCLowerCaseNames(names, field)
	if err != nil {
		return err
	}
	for _, name := range names {
		if configtypes.IsRPCRegistryReservedHeader(name) {
			return fmt.Errorf("%s: header %q is reserved", field, name)
		}
	}
	return nil
}

func validateRPCLowerCaseNames(names []string, field string) error {
	for _, name := range names {
		if name == "" {
			return fmt.Errorf("%s: empty name", field)
		}
		if name != strings.ToLower(name) {
			return fmt.Errorf("%s: name %q must be lower-case", field, name)
		}
	}
	return nil
}

// validateRPCRegistryTransforms checks grpc_status_to_code_transforms. The status name is parsed by grpc
// itself. The quotes make grpc accept names only, not numbers. The target is checked by the same helper
// as the HTTP status transforms.
func validateRPCRegistryTransforms(transforms configtypes.RPCGRPCStatusToCodeTransforms) error {
	for i, transform := range transforms {
		var code codes.Code
		err := code.UnmarshalJSON([]byte(strconv.Quote(transform.Status)))
		if err != nil {
			return fmt.Errorf("grpc_status_to_code_transforms[%d]: unknown gRPC status %q", i, transform.Status)
		}
		if code == codes.OK {
			return fmt.Errorf("grpc_status_to_code_transforms[%d]: %s cannot be transformed", i, transform.Status)
		}
		err = validateStatusToCodeTarget(rpcStatusToCodeTransformsKey, i, transform.ToError.TransformError, transform.ToDisconnect)
		if err != nil {
			return err
		}
		if (transform.ToError.AppCode != 0 || transform.ToError.AppType != "") && transform.ToError.Code == 0 {
			return fmt.Errorf("grpc_status_to_code_transforms[%d]: to_error.app_code and to_error.app_type need to_error.code", i)
		}
		if transform.ToError.AppType != "" && !configtypes.RPCAppErrorTypePattern.MatchString(transform.ToError.AppType) {
			return fmt.Errorf("grpc_status_to_code_transforms[%d]: to_error.app_type must match ^[A-Z0-9_]{1,64}$, got %q", i, transform.ToError.AppType)
		}
	}
	return nil
}
