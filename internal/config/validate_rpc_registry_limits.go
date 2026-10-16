package config

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// rpcRingHashSetRequestHashKeyEnv is the environment variable which gRPC reads to decide whether a ring_hash balancer
// hashes the value of its request header. When it is set to false, gRPC hashes at random.
const rpcRingHashSetRequestHashKeyEnv = "GRPC_EXPERIMENTAL_RING_HASH_SET_REQUEST_HASH_KEY"

// rpcResponseHintMaxField is the largest field number of a protobuf message, see protowire.MaxValidNumber.
const rpcResponseHintMaxField = 1<<29 - 1

var rpcAffinityHeaderRe = regexp.MustCompile(`^[a-z0-9_.-]{1,64}$`)

// validateRPCRegistryLimits checks the limits of an enabled registry which can never work: an admission cost over the
// budget, an affinity routing which cannot pick a replica, and a response size hint which bounds nothing. Limits which
// depend on bandwidth, latency or the window of an SDK are documented, not checked.
func validateRPCRegistryLimits(reg configtypes.RPCRegistry, file configtypes.RPCRegistryFile) error {
	checks := []func(configtypes.RPCRegistry, configtypes.RPCRegistryFile) error{
		validateRPCAdmissionBudget,
		validateRPCAffinity,
		validateRPCResponseSizeHints,
	}
	for _, check := range checks {
		err := check(reg, file)
		if err != nil {
			return err
		}
	}
	return nil
}

// rpcTargetsByName returns the targets of file by name.
func rpcTargetsByName(file configtypes.RPCRegistryFile) map[string]configtypes.RPCTarget {
	targets := make(map[string]configtypes.RPCTarget, len(file.Targets))
	for _, target := range file.Targets {
		targets[target.Name] = target
	}
	return targets
}

// validateRPCAdmissionBudget checks the admission keys. The budget must not be negative, and a method whose cost, its
// request limit plus its response limit, is over the budget could never run.
func validateRPCAdmissionBudget(reg configtypes.RPCRegistry, file configtypes.RPCRegistryFile) error {
	if reg.MaxInflightBytes < 0 {
		return fmt.Errorf("rpc.registry.max_inflight_bytes: must not be negative, got %d", reg.MaxInflightBytes)
	}
	if reg.MemoryWatermarkBytes < 0 {
		return fmt.Errorf("rpc.registry.memory_watermark_bytes: must not be negative, got %d", reg.MemoryWatermarkBytes)
	}
	if reg.MaxInflightBytes == 0 {
		return nil
	}
	targets := rpcTargetsByName(file)
	for _, method := range file.Methods {
		limits := configtypes.ResolveRPCMethodLimits(method, targets[method.Target], file.Defaults)
		cost := int64(limits.MaxRequestBytes) + int64(limits.MaxResponseBytes)
		if cost > reg.MaxInflightBytes {
			return fmt.Errorf("rpc registry method %q: cost %d bytes exceeds rpc.registry.max_inflight_bytes (%d): the method can never run", method.Method, cost, reg.MaxInflightBytes)
		}
	}
	return nil
}

// validateRPCAffinity checks the affinity routing of the targets: ring_hash and affinity_header come together, the
// header name is a valid one which a method forwards, and gRPC does not disable the hash key of the balancer.
func validateRPCAffinity(_ configtypes.RPCRegistry, file configtypes.RPCRegistryFile) error {
	usesRingHash := false
	for _, target := range file.Targets {
		if target.LoadBalancing != configtypes.RPCLoadBalancingRingHash {
			if target.AffinityHeader != "" {
				return fmt.Errorf("rpc registry target %q: affinity_header requires load_balancing: ring_hash", target.Name)
			}
			continue
		}
		usesRingHash = true
		err := validateRPCAffinityTarget(target, file.Methods)
		if err != nil {
			return err
		}
	}
	if usesRingHash && strings.EqualFold(os.Getenv(rpcRingHashSetRequestHashKeyEnv), "false") {
		return fmt.Errorf("rpc registry: a target uses ring_hash, but %s is false: gRPC would drop the affinity header and hash at random", rpcRingHashSetRequestHashKeyEnv)
	}
	return nil
}

// validateRPCAffinityTarget checks one ring_hash target: its header name, and that a method of the target forwards it.
func validateRPCAffinityTarget(target configtypes.RPCTarget, methods []configtypes.RPCMethod) error {
	name := target.AffinityHeader
	if name == "" {
		return fmt.Errorf("rpc registry target %q: ring_hash needs affinity_header", target.Name)
	}
	if !rpcAffinityHeaderRe.MatchString(name) {
		return fmt.Errorf("rpc registry target %q: affinity_header %q must match %s", target.Name, name, rpcAffinityHeaderRe.String())
	}
	if configtypes.IsRPCRegistryReservedHeader(name) || strings.HasSuffix(name, "-bin") {
		return fmt.Errorf("rpc registry target %q: affinity_header %q is reserved, or ends with -bin", target.Name, name)
	}
	forwarded := slices.ContainsFunc(methods, func(method configtypes.RPCMethod) bool {
		return method.Target == target.Name && slices.Contains(method.Headers, name)
	})
	if !forwarded {
		return fmt.Errorf("rpc registry target %q: no method forwards affinity_header %q", target.Name, name)
	}
	return nil
}

// validateRPCResponseSizeHints checks the response size hints of the methods: the field must be a valid field number,
// and the overhead must leave room under the response limit, so a hint can never bound a reply to nothing.
func validateRPCResponseSizeHints(_ configtypes.RPCRegistry, file configtypes.RPCRegistryFile) error {
	targets := rpcTargetsByName(file)
	for _, method := range file.Methods {
		hint := method.ResponseSizeHint
		if hint == nil {
			continue
		}
		if hint.RequestField < 1 || hint.RequestField > rpcResponseHintMaxField {
			return fmt.Errorf("rpc registry method %q: response_size_hint.request_field must be from 1 to %d, got %d", method.Method, rpcResponseHintMaxField, hint.RequestField)
		}
		if hint.OverheadBytes < 0 {
			return fmt.Errorf("rpc registry method %q: response_size_hint.overhead_bytes must not be negative, got %d", method.Method, hint.OverheadBytes)
		}
		overhead := hint.OverheadBytes
		if overhead == 0 {
			overhead = configtypes.DefaultResponseSizeOverheadBytes
		}
		limits := configtypes.ResolveRPCMethodLimits(method, targets[method.Target], file.Defaults)
		if overhead >= limits.MaxResponseBytes {
			return fmt.Errorf("rpc registry method %q: response_size_hint.overhead_bytes %d must be less than max_response_bytes %d", method.Method, overhead, limits.MaxResponseBytes)
		}
	}
	return nil
}
