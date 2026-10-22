-- Add presence information and emit the occupied channel state event.
-- The presence part is the body of presence_add.lua, unchanged.
-- KEYS[1] - presence zset key
-- KEYS[2] - presence hash key
-- KEYS[3] - per-user zset key
-- KEYS[4] - per-user hash key
-- KEYS[5] - channel state stream key
-- KEYS[6] - channel state pending zset key
-- KEYS[7] - channel state expire zset key
-- KEYS[8] - channel state kept hash key, see presence_state_check.lua
-- ARGV[1] - key expire seconds
-- ARGV[2] - expire at for set member
-- ARGV[3] - client ID
-- ARGV[4] - info payload
-- ARGV[5] - user ID
-- ARGV[6] - enable user mapping "0" or "1"
-- ARGV[7] - use hash field TTL "0" or "1"
-- ARGV[8] - channel
-- ARGV[9] - now in milliseconds
-- ARGV[10] - expire at in milliseconds (now plus presence TTL)
-- ARGV[11] - max stream length, 0 means no cap

-- The channel is occupied when its hash has clients, or when its expire entry is
-- still unresolved: a read may have cleaned the expired clients already, but no
-- vacated event has been armed or sent for them yet.
local occupied = redis.call("hlen", KEYS[2]) > 0 or redis.call("zscore", KEYS[7], ARGV[8]) ~= false

-- Check if client ID is new.
local isNewClient = false
if ARGV[6] ~= '0' then
    isNewClient = redis.call("hexists", KEYS[2], ARGV[3]) == 0
end

-- Add per-client presence.
redis.call("hset", KEYS[2], ARGV[3], ARGV[4])
redis.call("expire", KEYS[2], ARGV[1])
if ARGV[7] == '0' then
    redis.call("zadd", KEYS[1], ARGV[2], ARGV[3])
    redis.call("expire", KEYS[1], ARGV[1])
else
    redis.call("hexpire", KEYS[2], ARGV[1], "FIELDS", "1", ARGV[3])
end

-- Add per-user information.
if ARGV[6] ~= '0' then
    if isNewClient then
        redis.call("hincrby", KEYS[4], ARGV[5], 1)
    end
    redis.call("expire", KEYS[4], ARGV[1])
    if ARGV[7] == '0' then
        redis.call("zadd", KEYS[3], ARGV[2], ARGV[5])
        redis.call("expire", KEYS[3], ARGV[1])
    else
        redis.call("hexpire", KEYS[4], ARGV[1], "FIELDS", "1", ARGV[5])
    end
end

-- The presence of the channel can be gone at expire at; any add cancels a pending vacated event.
redis.call("zadd", KEYS[7], ARGV[10], ARGV[8])
local cancelled = redis.call("zrem", KEYS[6], ARGV[8])
if cancelled == 1 then
    redis.call("hdel", KEYS[8], ARGV[8])
end
if occupied or cancelled == 1 then
    return 0
end
if tonumber(ARGV[11]) > 0 and redis.call("xlen", KEYS[5]) >= tonumber(ARGV[11]) then
    return 2
end
redis.call("xadd", KEYS[5], "*", "c", ARGV[8], "t", "occupied", "ms", ARGV[9])
return 1
