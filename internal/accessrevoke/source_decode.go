package accessrevoke

import (
	"errors"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
)

// ErrUnknownMethod is returned when a source record has a method which is not an access revocation method.
var ErrUnknownMethod = errors.New("unknown access revocation method")

// decodeOperation decodes the JSON body of a source record into an Operation. The body has the same
// format as the request of the API command named by method, and the method names are the Kind values.
// It returns ErrUnknownMethod for methods which are not access revocation methods.
func decodeOperation(method string, body []byte) (Operation, error) {
	decoder := apiproto.NewJSONRequestDecoder()
	switch method {
	case KindBlockUser:
		req, err := decoder.DecodeBlockUser(body)
		if err != nil {
			return Operation{}, err
		}
		return Operation{
			Kind:     KindBlockUser,
			User:     req.User,
			ExpireAt: req.ExpireAt,
		}, nil
	case KindUnblockUser:
		req, err := decoder.DecodeUnblockUser(body)
		if err != nil {
			return Operation{}, err
		}
		return Operation{
			Kind: KindUnblockUser,
			User: req.User,
		}, nil
	case KindRevokeToken:
		req, err := decoder.DecodeRevokeToken(body)
		if err != nil {
			return Operation{}, err
		}
		return Operation{
			Kind:      KindRevokeToken,
			User:      req.User,
			Session:   req.Uid,
			RevokedAt: req.RevokedAt,
			ExpireAt:  req.ExpireAt,
		}, nil
	case KindInvalidateUserTokens:
		req, err := decoder.DecodeInvalidateUserTokens(body)
		if err != nil {
			return Operation{}, err
		}
		return Operation{
			Kind:         KindInvalidateUserTokens,
			User:         req.User,
			Channel:      req.Channel,
			Generation:   req.Generation,
			IssuedBefore: req.IssuedBefore,
			RevokedAt:    req.RevokedAt,
			ExpireAt:     req.ExpireAt,
		}, nil
	default:
		return Operation{}, ErrUnknownMethod
	}
}
