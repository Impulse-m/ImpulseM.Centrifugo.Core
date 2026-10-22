# Fork notes

This file holds the notes of the features that this fork adds to Core. The doc comments of the upstream options keep
their upstream text.

## Shared position sync

Without enableQueue, a large channel pays one full fan-out per publication and per insufficient state marker, under the
broadcast lock of the channel. Enable the queue for large channels.

## Client channel position check

Without SharedPositionSync on the channel medium, each subscriber with positioning reads the stream top of its channel
once per Config.ClientChannelPositionCheckDelay. A large channel with recovery should enable SharedPositionSync.

## Hash field TTL in Redis presence

Without UseHashFieldTTL, Presence removes the expired entries of a channel in one script call. That call blocks Redis for
as long as the removal takes when the channel has many expired entries. For large channels enable UseHashFieldTTL, so
that Redis expires the fields itself.

## Client tags filter limits

A client filter is limited in size by Config.ClientTagsFilterMaxNodes and Config.ClientTagsFilterMaxValues, when
SubscribeOptions.AllowTagsFilter is on. The node limit counts the nodes of the whole filter, and the value limit applies
to each comparison of the filter. Zero means the default of 128 nodes and 128 values per comparison, and a negative
value turns that limit off. A filter over a set limit is refused with the bad request error. The nesting of a client
filter is capped at 16 levels whatever the limits are. Raise the limits only for applications whose clients are trusted
with larger filters.

## Channel state

RedisChannelStateConfig.SweepInterval below 100ms is raised to 100ms when the manager is created, and a warning is logged.
Every partition of a node sweeps on this interval, so the Redis load grows with 1/SweepInterval.

A vacated event kept pending at the stream cap is retried after one SweepInterval, so the channels due behind it are
swept in the same step. The partition logs "channel state vacated event kept pending" at most once a minute while a
stream stays full. A dropped occupied event is counted in the events_dropped_count metric and logged at the same rate.

## Single flight

Config.UseSingleFlight shares a call between the callers that ask for the same key at the same time.

- A shared map read has a bound of 10 seconds. The bound ends the read only for brokers that honour the context of the
  call. The memory broker ignores the context, so its shared read runs until it returns.
- A shared read is not cancelled when its callers leave. A caller that leaves returns its own context error at once.
- Each caller that starts a shared call pays for two goroutines and a 10 second context timer. Callers that join a shared
  call pay for neither.
- A panic in the broker call is raised again on the goroutine of each caller, so a caller can recover it.
- A broker call that ends its goroutine with runtime.Goexit gives its callers the error "singleflight: shared call ended
  without a result", so no caller waits for a result that never comes.

## Backlog

The items below are deferred. Each one needs a measurement or a design before a change.

- Rebind holds connectMu. Measure the lock hold time under load before changing it.
- Presence add writes partition-wide zsets. Measure the write cost before changing the layout.
- Owned partitions are swept on idle ticks. This is polling and needs an event-driven design under the no-polling rule.
- A failing event blocks its partition. The sweep already runs outside the delivery retry loop. A dead-letter design
  is still open, and no dead-letter stream exists.
- The minimum LeaseTTL is 5ms, which gives a lease tick of 1ms. A floor that gives a tick of 100ms, a LeaseTTL of
  500ms, is not set, because the owner has not chosen the minimum and the tests use short leases. Below 500ms the
  lease loop runs more often than every 100ms, with the Redis calls of each renewal and heartbeat, so the
  RedisChannelStateConfig.LeaseTTL note says to use such values only in tests.
