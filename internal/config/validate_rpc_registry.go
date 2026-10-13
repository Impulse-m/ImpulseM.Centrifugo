package config

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"google.golang.org/grpc/codes"
)

// rpcStatusToCodeTransformsKey is the config key of the gRPC status transforms, used in messages.
const rpcStatusToCodeTransformsKey = "grpc_status_to_code_transforms"

var (
	rpcRegistryMethodRe     = regexp.MustCompile(`^[A-Za-z0-9_.:/-]{1,128}$`)
	rpcRegistryGRPCMethodRe = regexp.MustCompile(`^/[\w.]+/\w+$`)
)

// rpcRegistryReservedHeaders are names the registry sets itself. A client must not be able to
// forward them to a target, so they are rejected in both header allow-lists.
var rpcRegistryReservedHeaders = []string{
	"authorization",
	"traceparent",
	"tracestate",
	"x-forwarded-for",
}

const rpcRegistryReservedHeaderPrefix = "x-centrifugo-"

// validateRPCRegistry validates the RPC method registry. A disabled registry is not checked. An enabled one
// is checked in full, including the registry file, so checkconfig covers it.
func validateRPCRegistry(c Config) error {
	err := validateRPCRegistryTransforms(c.RPC.Registry.GRPCStatusToCodeTransforms)
	if err != nil {
		return fmt.Errorf("in rpc.registry: %w", err)
	}
	if !c.RPC.Registry.Enabled {
		return nil
	}
	_, err = ResolveRPCRegistry(c.RPC.Registry)
	return err
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
		if slices.Contains(rpcRegistryReservedHeaders, name) || strings.HasPrefix(name, rpcRegistryReservedHeaderPrefix) {
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
		err = validateStatusToCodeTarget(rpcStatusToCodeTransformsKey, i, transform.ToError, transform.ToDisconnect)
		if err != nil {
			return err
		}
	}
	return nil
}
