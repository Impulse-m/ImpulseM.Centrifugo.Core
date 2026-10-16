-- Release the lease of a channel state partition when this acquisition still holds it.
-- KEYS[1] - lease key
-- ARGV[1] - token the acquisition wrote to the lease key
-- Returns 1 when released, 0 when another acquisition holds the lease or it has expired.

if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0
