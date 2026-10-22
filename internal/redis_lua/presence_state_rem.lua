-- Remove client presence and arm the vacated channel state event when the channel has no clients left.
-- The presence part is the body of presence_rem.lua, unchanged.
-- KEYS[1] - presence set key
-- KEYS[2] - presence hash key
-- KEYS[3] - per-user zset key
-- KEYS[4] - per-user hash key
-- KEYS[5] - channel state pending zset key
-- KEYS[6] - channel state expire zset key
-- ARGV[1] - client ID
-- ARGV[2] - user ID
-- ARGV[3] - enable user mapping "0" or "1"
-- ARGV[4] - use hash field TTL "0" or "1"
-- ARGV[5] - channel
-- ARGV[6] - now in seconds
-- ARGV[7] - fire at in milliseconds of the vacated event

-- Only a client that was in the hash arms the vacated event: a repeated remove must not send another one.
local existed = redis.call("hexists", KEYS[2], ARGV[1]) == 1

local clientExists = false
if ARGV[3] ~= '0' then
    -- Check if client ID exists in hash.
    clientExists = redis.call("hexists", KEYS[2], ARGV[1]) == 1
end

redis.call("hdel", KEYS[2], ARGV[1])
if ARGV[4] == '0' then
    redis.call("zrem", KEYS[1], ARGV[1])
end

if ARGV[3] ~= '0' and clientExists then
    local connectionsCount = redis.call("hincrby", KEYS[4], ARGV[2], -1)
    -- If the number of connections for this user is zero, remove the user
    -- from the sorted set and clean hash.
    if connectionsCount <= 0 then
        if ARGV[4] == '0' then
            redis.call("zrem", KEYS[3], ARGV[2])
        end
        redis.call("hdel", KEYS[4], ARGV[2])
    end
end

-- Expired entries of dead nodes do not keep a channel occupied (same cleanup as presence_get.lua, but at most
-- 1000 entries per call, so one call does not block Redis on a large set).
local more = false
if ARGV[4] == '0' then
    local expired = redis.call("zrangebyscore", KEYS[1], "0", ARGV[6], "LIMIT", 0, 1000)
    if #expired > 0 then
        for num = 1, #expired do
            redis.call("hdel", KEYS[2], expired[num])
        end
        redis.call("zrem", KEYS[1], unpack(expired))
    end
    more = #expired >= 1000
end

-- The vacated event always goes through the pending zset, so a quick reconnect cancels it.
if existed and redis.call("hlen", KEYS[2]) == 0 then
    redis.call("zadd", KEYS[5], ARGV[7], ARGV[5])
    redis.call("zrem", KEYS[6], ARGV[5])
end
-- A full batch may leave expired entries: the sweep checks the channel again at once.
if more and redis.call("hlen", KEYS[2]) > 0 then
    redis.call("zadd", KEYS[6], tonumber(ARGV[6]) * 1000, ARGV[5])
end
return 0
