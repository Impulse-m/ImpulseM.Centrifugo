package accessrevoke

import (
	"errors"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
)

// ErrUnknownMethod is returned when a source record has a method which is not an access revocation method.
var ErrUnknownMethod = errors.New("unknown access revocation method")

// Method names of access revocation records. They are the same as API command method names.
const (
	methodBlockUser            = "block_user"
	methodUnblockUser          = "unblock_user"
	methodRevokeToken          = "revoke_token"
	methodInvalidateUserTokens = "invalidate_user_tokens"
)

// decodeOperation decodes the JSON body of a source record into an Operation. The body has the same
// format as the request of the API command named by method. It returns ErrUnknownMethod for methods
// which are not access revocation methods.
func decodeOperation(method string, body []byte) (Operation, error) {
	decoder := apiproto.NewJSONRequestDecoder()
	switch method {
	case methodBlockUser:
		req, err := decoder.DecodeBlockUser(body)
		if err != nil {
			return Operation{}, err
		}
		return Operation{
			Kind:     KindBlockUser,
			User:     req.User,
			ExpireAt: req.ExpireAt,
		}, nil
	case methodUnblockUser:
		req, err := decoder.DecodeUnblockUser(body)
		if err != nil {
			return Operation{}, err
		}
		return Operation{
			Kind: KindUnblockUser,
			User: req.User,
		}, nil
	case methodRevokeToken:
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
	case methodInvalidateUserTokens:
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
