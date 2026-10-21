-- Fixed window counter check. KEYS[1] is a hash with two words per bucket of a scope: the window index and the count
-- of that window. ARGV is described in windowArgs. The clock is Redis TIME, in microseconds. All buckets are checked
-- first and written only when every bucket allows the operation. A refusal returns the largest wait of the refusing
-- buckets as a negative number of microseconds, and writes nothing. Returns 1 when allowed.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local n = tonumber(ARGV[2])
local fields = {}
for i = 1, 2 * n do
	fields[i] = tostring(i - 1)
end
local stored = redis.call('HMGET', KEYS[1], unpack(fields))
local updates = {}
local worst = 0
for i = 1, n do
	local window = tonumber(ARGV[2 * i + 1])
	local rate = tonumber(ARGV[2 * i + 2])
	local base = 2 * (i - 1)
	local storedIdx = tonumber(stored[2 * i - 1]) or 0
	local idx = floorDiv(now, window)
	if storedIdx == idx + 1 then
		idx = storedIdx
	end
	local cur = 0
	if storedIdx == idx then
		cur = tonumber(stored[2 * i]) or 0
	end
	if cur < rate then
		updates[#updates + 1] = tostring(base)
		updates[#updates + 1] = string.format('%.0f', idx)
		updates[#updates + 1] = tostring(base + 1)
		updates[#updates + 1] = string.format('%.0f', cur + 1)
	else
		local wait = (idx + 1) * window - now
		if wait > worst then
			worst = wait
		end
	end
end
