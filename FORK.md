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
SubscribeOptions.AllowTagsFilter is on. Zero selects the defaults of 256 nodes and 1024 values. A negative value means
no limit. A filter over the limit is refused with the bad request error.

## Single flight

Config.UseSingleFlight shares a call between the callers that ask for the same key at the same time.

- A shared map read has a bound of 10 seconds. The bound ends the read only for brokers that honour the context of the
  call. The memory broker ignores the context, so its shared read runs until it returns.
- A shared read is not cancelled when its callers leave. A caller that leaves returns its own context error at once.
- Each caller that starts a shared call pays for a goroutine and a 10 second context timer. Callers that join a shared
  call pay for neither.
- A panic in the broker call is raised again on the goroutine of each caller, so a caller can recover it.
- A broker call that ends its goroutine with runtime.Goexit gives its callers no result. They wait until their own
  contexts end.

## Backlog

The items below are deferred. Each one needs a measurement or a design before a change.

- Rebind holds connectMu. Measure the lock hold time under load before changing it.
- Presence add writes partition-wide zsets. Measure the write cost before changing the layout.
- Owned partitions are swept on idle ticks. This is polling and needs an event-driven design under the no-polling rule.
- A failing event blocks its partition. The sweep already runs outside the delivery retry loop. A dead-letter design
  is still open, and no dead-letter stream exists.
- A broker call that ends its goroutine with runtime.Goexit leaves its callers waiting for their own contexts. A fix
  needs a goroutine per shared call or a bound on the caller side.
