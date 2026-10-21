-- Sliding window counter check. KEYS[1] is a hash with three words per bucket of a scope: the window index, the count
-- of that window and the count of the previous window. ARGV is described in windowArgs. The clock is Redis TIME, in
-- microseconds. All buckets are checked first and written only when every bucket allows the operation. A refusal
-- returns the largest wait of the refusing buckets as a negative number of microseconds, and writes nothing.
-- Returns 1 when allowed.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local n = tonumber(ARGV[2])
local fields = {}
for i = 1, 3 * n do
	fields[i] = tostring(i - 1)
end
local stored = redis.call('HMGET', KEYS[1], unpack(fields))

-- slidingWait returns the wait of a refused operation: the first offset in the window where the operation is allowed,
-- or the start of the next window when the current one never allows it. Its formulas match the Go implementation.
local function slidingWait(window, rate, prev, cur, e)
	local b = rate - cur
	if b >= 1 and prev > 0 then
		local q = ceilDiv(b * window, prev)
		if q >= 2 then
			return window - q + 1 - e
		end
	end
	if cur == 0 then
		return window - e
	end
	local q = ceilDiv(rate * window, cur)
	return window - e + math.max(0, window - q + 1)
end

local updates = {}
local worst = 0
for i = 1, n do
	local window = tonumber(ARGV[2 * i + 1])
	local rate = tonumber(ARGV[2 * i + 2])
	local base = 3 * (i - 1)
	local storedIdx = tonumber(stored[3 * i - 2]) or 0
	local storedCur = tonumber(stored[3 * i - 1]) or 0
	local storedPrev = tonumber(stored[3 * i]) or 0
	local idx = floorDiv(now, window)
	if storedIdx == idx + 1 then
		idx = storedIdx
	end
	local e = now - idx * window
	local e0 = math.max(e, 0)
	local prev = 0
	local cur = 0
	if storedIdx == idx then
		prev = storedPrev
		cur = storedCur
	elseif storedIdx == idx - 1 then
		prev = storedCur
	end
	if prev * (window - e0) < (rate - cur) * window then
		updates[#updates + 1] = tostring(base)
		updates[#updates + 1] = string.format('%.0f', idx)
		updates[#updates + 1] = tostring(base + 1)
		updates[#updates + 1] = string.format('%.0f', cur + 1)
		updates[#updates + 1] = tostring(base + 2)
		updates[#updates + 1] = string.format('%.0f', prev)
	else
		local wait = slidingWait(window, rate, prev, cur, e)
		if wait > worst then
			worst = wait
		end
	end
end
