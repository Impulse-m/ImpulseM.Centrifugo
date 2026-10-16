package rpcdedup

import (
	"github.com/redis/rueidis"
)

// beginScript looks a key up and starts a call in it. KEYS[1] is the key of the call. ARGV[1] is the fingerprint,
// ARGV[2] the node id of the owner and ARGV[3] the in-flight TTL in milliseconds. It returns {'miss'} when it started
// the call, {'cancelled', error} for a tombstone, which answers any fingerprint, {'done', reply, error, uncached} for a
// kept answer, {'mismatch'} for another fingerprint, and {'inflight', owner} for a call which runs. ARGV[4] is "1" when
// the caller waits for the call on another node, and then the caller is registered as a waiter in field w, the node ids
// of the waiters, space-separated, at most 8 of them: the owner notifies them when it writes the record. Scripts read
// before they write and never call TIME, so they replicate and run on Redis 6 and later.
var beginScript = rueidis.NewLuaScript(`
local r = redis.call('HMGET', KEYS[1], 's', 'f', 'n', 'r', 'e', 'u', 'w')
if r[1] == false then
	redis.call('HSET', KEYS[1], 's', 'i', 'f', ARGV[1], 'n', ARGV[2])
	redis.call('PEXPIRE', KEYS[1], ARGV[3])
	return {'miss'}
end
if r[1] == 'c' then
	return {'cancelled', r[5] or ''}
end
if r[2] ~= ARGV[1] then
	return {'mismatch'}
end
if r[1] == 'i' then
	if ARGV[4] == '1' then
		local w = r[7]
		if w == false or w == '' then
			redis.call('HSET', KEYS[1], 'w', ARGV[2])
		elseif not string.find(' ' .. w .. ' ', ' ' .. ARGV[2] .. ' ', 1, true) then
			local _, n = string.gsub(w, ' ', '')
			if n + 1 < 8 then
				redis.call('HSET', KEYS[1], 'w', w .. ' ' .. ARGV[2])
			end
		end
	end
	return {'inflight', r[3]}
end
return {'done', r[4] or '', r[5] or '', r[6] or ''}
`)

// completeScript keeps the answer of an in-flight call. KEYS[1] is the key of the call. ARGV[1] is the node id of
// the owner, ARGV[2] the reply, ARGV[3] the error JSON, ARGV[4] uncached ("1" or "0"), ARGV[5] the record TTL in
// milliseconds and ARGV[6] abandoned ("1" or "0"). Only the owner completes the record it began, and it returns 0
// otherwise. An abandoned call deletes its record, so a duplicate runs at once. It returns {owned, waiters}: owned is
// "1" when the owner wrote the record, and waiters are the node ids of its waiters, which the caller notifies. A
// record which turns done drops its waiters.
var completeScript = rueidis.NewLuaScript(`
local r = redis.call('HMGET', KEYS[1], 's', 'n', 'w')
if r[1] ~= 'i' or r[2] ~= ARGV[1] then
	return {'0', ''}
end
local w = r[3] or ''
if ARGV[6] == '1' then
	redis.call('DEL', KEYS[1])
	return {'1', w}
end
redis.call('HDEL', KEYS[1], 'n', 'w')
if ARGV[4] == '1' then
	redis.call('HSET', KEYS[1], 's', 'd', 'u', '1')
else
	redis.call('HSET', KEYS[1], 's', 'd', 'r', ARGV[2], 'e', ARGV[3])
end
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return {'1', w}
`)

// cancelScript cancels a call of KEYS[1]. ARGV[1] is the error JSON of the cancelled error and ARGV[2] the record TTL
// in milliseconds. The script stores the error with no size check: config validation checks the encoded cancelled error
// against configtypes.RPCDedup.ReplyLimit, so a tombstone fits the reply limit of both tiers. A key which holds nothing
// becomes a tombstone, {'tombstone'}. A call which runs returns its owner,
// {'inflight', owner}: the owner is told by a notification, and completes the record itself. A done key is left as
// it is, {'done'}.
var cancelScript = rueidis.NewLuaScript(`
local r = redis.call('HMGET', KEYS[1], 's', 'n')
if r[1] == false then
	redis.call('HSET', KEYS[1], 's', 'c', 'e', ARGV[1])
	redis.call('PEXPIRE', KEYS[1], ARGV[2])
	return {'tombstone'}
end
if r[1] == 'i' then
	return {'inflight', r[2]}
end
return {'done'}
`)

// ackScript deletes the done records and tombstones of the keys of one scope, given as KEYS. An in-flight record is
// never deleted. It returns the number of deleted keys.
var ackScript = rueidis.NewLuaScript(`
local n = 0
for i = 1, #KEYS do
	if redis.call('HGET', KEYS[i], 's') ~= 'i' then
		n = n + redis.call('DEL', KEYS[i])
	end
end
return n
`)
