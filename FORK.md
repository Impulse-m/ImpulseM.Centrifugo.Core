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

Cost of a full stream: while the stream of a partition stays at the cap, every kept vacated event is checked once per
SweepInterval, with one check script and one write. A sweep reads up to 10 pages of 100 due channels per partition, so
a partition with many kept channels spends its sweep on them. There is no backoff per channel yet (see Backlog).

Cost of a failing delivery: the sweep runs once per backoff step, not once per SweepInterval. A batch that keeps failing
is delivered again after a delay that doubles up to 20 seconds, and the partition is swept once per delay. So under a
persistent failure a kept vacated event waits up to that delay for its retry.

Lease loop timing: a worker writes its heartbeat at most once per LeaseTTL/3, on the first lease tick after the interval
has passed. With the default ticks, two heartbeats come up to about 0.4 * LeaseTTL apart. Under a slow shard the gap can
reach about one LeaseTTL: a cycle can start up to two call timeouts after the one before it, and its heartbeat can land
up to one call timeout after its start. A worker that is counted out for that long is left out of the share of the other
workers for about one cycle, so they take a few more partitions meanwhile. The lease is the guard: it keeps one consumer
per partition. A cycle in between uses the count of the last one. The heartbeat runs only when it is due, beside the
renewals. The cycle waits for the renewals and the heartbeat for at most a call timeout, LeaseTTL/3. A shard that does
not answer in time is given up, so it cannot delay the renewals of the others. The acquisitions wait for at most a call
timeout too, so a cycle lasts at most two call timeouts, two thirds of LeaseTTL, however long a shard takes to answer.

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
- A failing event blocks its partition. The sweep runs once per backoff step (see Cost of a failing delivery), not on
  every tick. A dead-letter design is still open, and no dead-letter stream exists.
- Open, owner decision: the minimum LeaseTTL. The minimum is 5ms, which gives a lease tick of 1ms, so a LeaseTTL of 5ms
  runs the lease loop about 1000 times per second. A floor, for example a LeaseTTL of 500ms with a tick of 100ms, is not
  set, because the owner has not chosen the minimum. Any floor must leave every default unchanged. The
  RedisChannelStateConfig.LeaseTTL note says to use values below 500ms only in tests.
- Open, owner decision: the 100ms floor of SweepInterval. An explicit value below it is raised to 100ms with a warning.
  The owner has not confirmed that floor, or refusing such values at config time instead.
- A kept vacated event is checked once per SweepInterval while its stream stays at the cap. A backoff per channel needs
  an attempt count, and no measurement of the cost exists yet.
