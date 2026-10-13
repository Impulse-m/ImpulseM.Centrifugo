package proxy

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
)

// statusRule is a status to protocol error or disconnect rule. Status is the key of the transform
// kind: an HTTP status code or a gRPC status code.
type statusRule[K comparable] struct {
	status       K
	toError      configtypes.TransformError
	toDisconnect configtypes.TransformDisconnect
}

// matchStatusRule returns the protocol error or disconnect of the first rule which matches status and
// sets a code. ok is false when no rule matches.
func matchStatusRule[K comparable](status K, rules []statusRule[K]) (*proxyproto.Error, *proxyproto.Disconnect, bool) {
	for _, rule := range rules {
		if rule.status != status {
			continue
		}
		if rule.toError.Code > 0 {
			return &proxyproto.Error{
				Code:      rule.toError.Code,
				Message:   rule.toError.Message,
				Temporary: rule.toError.Temporary,
			}, nil, true
		}
		if rule.toDisconnect.Code > 0 {
			return nil, &proxyproto.Disconnect{
				Code:   rule.toDisconnect.Code,
				Reason: rule.toDisconnect.Reason,
			}, true
		}
	}
	return nil, nil, false
}

// httpStatusRules converts HTTP status transforms to status rules.
func httpStatusRules(transforms []configtypes.HttpStatusToCodeTransform) []statusRule[int] {
	rules := make([]statusRule[int], 0, len(transforms))
	for _, t := range transforms {
		rules = append(rules, statusRule[int]{
			status:       t.StatusCode,
			toError:      t.ToError,
			toDisconnect: t.ToDisconnect,
		})
	}
	return rules
}
