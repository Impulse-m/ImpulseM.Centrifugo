package rpcregistry

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// authClassNames holds the name of each auth class, as the config names it. String reads it.
var authClassNames = [...]struct {
	class AuthClass
	name  string
}{
	{class: AuthClassAnonymous, name: configtypes.RPCAuthClassAnonymous},
	{class: AuthClassPending2FA, name: configtypes.RPCAuthClassPending2FA},
	{class: AuthClassAuthenticated, name: configtypes.RPCAuthClassAuthenticated},
	{class: AuthClassBot, name: configtypes.RPCAuthClassBot},
}

// String returns the name of the class, as the config names it. The zero value has no name.
func (a AuthClass) String() string {
	for _, entry := range authClassNames {
		if entry.class == a {
			return entry.name
		}
	}
	return ""
}
