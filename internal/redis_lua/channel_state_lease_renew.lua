-- Renew the lease of a channel state partition when this node still holds it.
-- KEYS[1] - lease key
-- ARGV[1] - node ID of the owner
-- ARGV[2] - lease TTL in milliseconds
-- Returns 1 when renewed, 0 when another owner holds the lease or it has expired.

if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0
