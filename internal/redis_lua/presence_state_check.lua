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
-- ARGV[8] - expired entries removed per call
-- ARGV[9] - sweep interval in milliseconds, the delay of a vacated event kept pending
-- Returns 0 when nothing is written, 1 when the vacated event is written to the stream, and 3 when the event is
-- kept pending because the stream is at its cap. A kept event is moved one sweep interval ahead in the pending set,
-- so the sweep of the partition moves on to the channels due behind it.

local more = false
if ARGV[5] == '0' then
    -- Expired entries do not keep a channel occupied (same cleanup as presence_get.lua, but at most
    -- ARGV[8] entries per call, so one call does not block Redis on a large set).
    local expired = redis.call("zrangebyscore", KEYS[1], "0", ARGV[3], "LIMIT", 0, ARGV[8])
    if #expired > 0 then
        for num = 1, #expired do
            redis.call("hdel", KEYS[2], expired[num])
        end
        redis.call("zrem", KEYS[1], unpack(expired))
    end
    more = #expired >= tonumber(ARGV[8])
end

if redis.call("hlen", KEYS[2]) > 0 then
    -- Occupied: no vacated event is pending, the presence can expire at the upper bound. A full batch
    -- may leave expired entries, so the channel is checked again by the next sweep. It is armed one
    -- millisecond ahead, so the sweep that runs this script does not select the channel again.
    redis.call("zrem", KEYS[4], ARGV[1])
    local expireAt = tonumber(ARGV[2]) + tonumber(ARGV[7])
    if more then
        expireAt = tonumber(ARGV[2]) + 1
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
    -- Kept pending at the cap: the next sweep retries it after one sweep interval, and sends the event once the stream
    -- has room. Until then the entry is not due, so the channels behind it are swept.
    redis.call("zadd", KEYS[4], tonumber(ARGV[2]) + tonumber(ARGV[9]), ARGV[1])
    return 3
end
redis.call("zrem", KEYS[4], ARGV[1])
redis.call("xadd", KEYS[3], "*", "c", ARGV[1], "t", "vacated", "ms", ARGV[2])
return 1
