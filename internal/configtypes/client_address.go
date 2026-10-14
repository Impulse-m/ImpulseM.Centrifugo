package configtypes

// ClientAddress is a configuration for the client address behind trusted proxies: which
// forwarded header is believed and which PROXY protocol peers are trusted.
type ClientAddress struct {
	// Enabled turns on client address resolution. When false, nothing else in this section is used.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" expose:"full" doc:"Turns on client address resolution. When false, nothing else in this section is used."`
	// TrustedProxies is a list of IP addresses or CIDR ranges of proxies whose forwarded header and
	// PROXY protocol header are believed. Nothing is trusted unless listed. With PROXY protocol the
	// address the PROXY header reports is the previous hop, and it decides whether the forwarded header
	// is read. A chain of a proxy, then a balancer which sends PROXY, needs both listed: the balancer
	// because it is the socket peer, and the proxy because its forwarded header is read.
	TrustedProxies []string `mapstructure:"trusted_proxies" json:"trusted_proxies" envconfig:"trusted_proxies" yaml:"trusted_proxies" toml:"trusted_proxies" expose:"full" doc:"A list of IP addresses or CIDR ranges of proxies whose forwarded header and PROXY protocol header are believed. Nothing is trusted unless listed. With PROXY protocol the address the PROXY header reports is the previous hop, and it decides whether the forwarded header is read. A chain of a proxy, then a balancer which sends PROXY, needs both listed: the balancer because it is the socket peer, and the proxy because its forwarded header is read."`
	// Header is a name of the forwarded header read from a trusted proxy. Its values are walked
	// right to left, skipping trusted proxies.
	Header string `mapstructure:"header" default:"x-forwarded-for" json:"header" envconfig:"header" yaml:"header" toml:"header" expose:"full" doc:"A name of the forwarded header read from a trusted proxy. Its values are walked right to left, skipping trusted proxies."`
	// ProxyProtocol is a configuration for PROXY protocol on the client-facing ports.
	ProxyProtocol ClientAddressProxyProtocol `mapstructure:"proxy_protocol" json:"proxy_protocol" envconfig:"proxy_protocol" yaml:"proxy_protocol" toml:"proxy_protocol" expose:"full" doc:"A configuration for PROXY protocol on the client-facing ports: the external HTTP port and uni_grpc."`
}

// ClientAddressProxyProtocol is a configuration for PROXY protocol v1 and v2 headers.
type ClientAddressProxyProtocol struct {
	// Enabled reads PROXY protocol headers on the client-facing ports. Requires client_address.enabled
	// and a non-empty client_address.trusted_proxies.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" expose:"full" doc:"Reads PROXY protocol headers on the client-facing ports. Requires client_address.enabled and a non-empty client_address.trusted_proxies."`
	// UntrustedPeers is what to do with a connection from a peer which is not in trusted_proxies and
	// sends a PROXY header: reject closes it, ignore discards the header. A connection from such a peer
	// without a header uses its socket address either way.
	UntrustedPeers string `mapstructure:"untrusted_peers" default:"reject" json:"untrusted_peers" envconfig:"untrusted_peers" yaml:"untrusted_peers" toml:"untrusted_peers" expose:"full" doc:"What to do with a connection from a peer which is not in trusted_proxies and sends a PROXY header: reject closes it, ignore discards the header. A connection from such a peer without a header uses its socket address either way. Can be <<reject>> or <<ignore>>."`
	// ReadHeaderTimeout is how long a new connection may take to send its PROXY protocol header.
	ReadHeaderTimeout Duration `mapstructure:"read_header_timeout" default:"10s" json:"read_header_timeout" envconfig:"read_header_timeout" yaml:"read_header_timeout" toml:"read_header_timeout" expose:"full" doc:"How long a new connection may take to send its PROXY protocol header."`
}
