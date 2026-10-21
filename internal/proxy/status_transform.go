package proxy

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
)

// statusRule is a status to protocol error or disconnect rule. Status is the key of the transform
// kind: an HTTP status code or a gRPC status code. Only gRPC rules set the application fields of toError.
type statusRule[K comparable] struct {
	status       K
	toError      configtypes.RPCTransformError
	toDisconnect configtypes.TransformDisconnect
}

// findStatusRule returns the first rule which matches status and sets a code. ok is false when no rule matches.
func findStatusRule[K comparable](status K, rules []statusRule[K]) (statusRule[K], bool) {
	for _, rule := range rules {
		if rule.status != status {
			continue
		}
		if rule.toError.Code > 0 || rule.toDisconnect.Code > 0 {
			return rule, true
		}
	}
	return statusRule[K]{}, false
}

// statusRuleResult returns the protocol error of a rule found by findStatusRule, or its disconnect when
// the rule sets no error code.
func statusRuleResult[K comparable](rule statusRule[K]) (*proxyproto.Error, *proxyproto.Disconnect) {
	if rule.toError.Code > 0 {
		return &proxyproto.Error{
			Code:      rule.toError.Code,
			Message:   rule.toError.Message,
			Temporary: rule.toError.Temporary,
		}, nil
	}
	return nil, &proxyproto.Disconnect{
		Code:   rule.toDisconnect.Code,
		Reason: rule.toDisconnect.Reason,
	}
}

// matchStatusRule returns the protocol error or disconnect of the first rule which matches status and
// sets a code. ok is false when no rule matches.
func matchStatusRule[K comparable](status K, rules []statusRule[K]) (*proxyproto.Error, *proxyproto.Disconnect, bool) {
	rule, ok := findStatusRule(status, rules)
	if !ok {
		return nil, nil, false
	}
	protocolError, protocolDisconnect := statusRuleResult(rule)
	return protocolError, protocolDisconnect, true
}

// httpStatusRules converts HTTP status transforms to status rules.
func httpStatusRules(transforms []configtypes.HttpStatusToCodeTransform) []statusRule[int] {
	rules := make([]statusRule[int], 0, len(transforms))
	for _, t := range transforms {
		rules = append(rules, statusRule[int]{
			status:       t.StatusCode,
			toError:      configtypes.RPCTransformError{TransformError: t.ToError},
			toDisconnect: t.ToDisconnect,
		})
	}
	return rules
}
