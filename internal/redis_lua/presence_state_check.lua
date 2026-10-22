-- Sweep one channel of a partition: emit the vacated channel state event when its delay has
-- passed without clients, or arm the delay when its presence has expired.
-- KEYS[1] - presence zset key
-- KEYS[2] - presence hash key
-- KEYS[3] - channel state stream key
-- KEYS[4] - channel state pending zset key
-- KEYS[5] - channel state expire zset key
-- ARGV[1] - channel
-- ARGV[2] - now in milliseconds
-- ARGV[3] - now in seconds
-- ARGV[4] - fire at in milliseconds, used when the delay is armed now
-- ARGV[5] - use hash field TTL "0" or "1"
-- ARGV[6] - max stream length, 0 means no cap
-- ARGV[7] - presence TTL in milliseconds

local more = false
if ARGV[5] == '0' then
    -- Expired entries do not keep a channel occupied (same cleanup as presence_get.lua, but at most
    -- 1000 entries per call, so one call does not block Redis on a large set).
    local expired = redis.call("zrangebyscore", KEYS[1], "0", ARGV[3], "LIMIT", 0, 1000)
    if #expired > 0 then
        for num = 1, #expired do
            redis.call("hdel", KEYS[2], expired[num])
        end
        redis.call("zrem", KEYS[1], unpack(expired))
    end
    more = #expired >= 1000
end

if redis.call("hlen", KEYS[2]) > 0 then
    -- Occupied: no vacated event is pending, the presence can expire at the upper bound. A full batch
    -- may leave expired entries, so the channel is checked again at once.
    redis.call("zrem", KEYS[4], ARGV[1])
    local expireAt = tonumber(ARGV[2]) + tonumber(ARGV[7])
    if more then
        expireAt = tonumber(ARGV[2])
    end
    redis.call("zadd", KEYS[5], expireAt, ARGV[1])
    return 0
end

-- The expire entry is removed by the sweep when it resolves the channel, so a missing entry
-- means another sweep resolved the channel already: a stale check must not arm a new delay.
local hadExpire = redis.call("zrem", KEYS[5], ARGV[1]) == 1
local fire = redis.call("zscore", KEYS[4], ARGV[1])
if not fire then
    if hadExpire then
        -- The presence of the channel expired: arm the delay.
        redis.call("zadd", KEYS[4], ARGV[4], ARGV[1])
    end
    return 0
end
if tonumber(fire) > tonumber(ARGV[2]) then
    return 0
end

if tonumber(ARGV[6]) > 0 and redis.call("xlen", KEYS[3]) >= tonumber(ARGV[6]) then
    -- Dropped at the cap: the pending entry stays, so the next sweep sends the event once the stream has room.
    return 2
end
redis.call("zrem", KEYS[4], ARGV[1])
redis.call("xadd", KEYS[3], "*", "c", ARGV[1], "t", "vacated", "ms", ARGV[2])
return 1
