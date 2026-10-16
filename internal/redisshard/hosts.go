package redisshard

import (
	"github.com/redis/rueidis"
)

// InitAddresses returns the host addresses which a connection address connects to, as optionsFromAddress reads
// them: the host of a Redis URL and its addr query parameters, or the address itself when it is host:port. An
// address which optionsFromAddress rejects is returned as written, so that a configuration validator can compare
// the hosts of every configured address.
func InitAddresses(address string) []string {
	opts, err := optionsFromAddress(address, rueidis.ClientOption{})
	if err != nil {
		return []string{address}
	}
	return opts.ClientOption.InitAddress
}
