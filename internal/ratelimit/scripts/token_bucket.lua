-- Token bucket (GCRA) check. KEYS[1] is a hash with one theoretical arrival time (TAT, microseconds) per bucket of a
-- scope, fields are bucket indexes starting from 0. ARGV is described in tokenBucket.redisArgs. All buckets are checked
-- first and TATs are written only when every bucket allows the operation. A refusal returns the largest wait of the
-- refusing buckets as a negative number of microseconds, and writes nothing. Returns 1 when allowed.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local n = tonumber(ARGV[2])
local fields = {}
for i = 1, n do
	fields[i] = tostring(i - 1)
end
local stored = redis.call('HMGET', KEYS[1], unpack(fields))
local updates = {}
local worst = 0
for i = 1, n do
	local emission = tonumber(ARGV[2 * i + 1])
	local tolerance = tonumber(ARGV[2 * i + 2])
	local tat = tonumber(stored[i]) or 0
	if tat < now then
		tat = now
	end
	tat = tat + emission
	local over = tat - now - tolerance
	if over > worst then
		worst = over
	end
	updates[2 * i - 1] = fields[i]
	updates[2 * i] = string.format('%.0f', tat)
end
if worst > 0 then
	return -worst
end
redis.call('HSET', KEYS[1], unpack(updates))
redis.call('PEXPIRE', KEYS[1], ARGV[1])
return 1
