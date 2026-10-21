package config

func applyConfigMigrations(cfg Config) (Config, []string) {
	var warnings []string
	rl := &cfg.Client.RateLimit
	warnings = migrateRateLimitMethodOverride(&rl.ClientCommand.RPC, "client.rate_limit.client_command.rpc", warnings)
	warnings = migrateRateLimitMethodOverride(&rl.UserCommand.RPC, "client.rate_limit.user_command.rpc", warnings)
	warnings = migrateRateLimitMethodOverride(&rl.RedisUserCommand.RPC, "client.rate_limit.redis_user_command.rpc", warnings)
	return cfg, warnings
}
