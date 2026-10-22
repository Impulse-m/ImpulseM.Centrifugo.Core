package centrifuge

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	_ "embed"

	"github.com/redis/rueidis"
)

const (
	channelStateDefaultNumPartitions     = 128
	channelStateDefaultVacatedEventDelay = 5 * time.Second
	channelStateDefaultMaxStreamLength   = 8192
	channelStateDefaultSweepInterval     = time.Second
	channelStateDefaultLeaseTTL          = 5 * time.Second
	channelStateDefaultBatchSize         = 100
	channelStateMinLeaseTTL              = 5 * time.Millisecond
	channelStateMinSweepInterval         = 100 * time.Millisecond
	channelStateSweepPageSize            = 100
	channelStateSweepMaxPages            = 10
	// channelStateExpiredBatch bounds the expired entries that one state script removes from the presence set.
	channelStateExpiredBatch = 1000
	// channelStateKeptPending is the result of the check script for a vacated event kept pending at the stream cap.
	channelStateKeptPending = 3
)

var (
	//go:embed internal/redis_lua/presence_state_add.lua
	presenceStateAddScriptSource string

	//go:embed internal/redis_lua/presence_state_rem.lua
	presenceStateRemScriptSource string

	//go:embed internal/redis_lua/presence_state_check.lua
	presenceStateCheckScriptSource string

	//go:embed internal/redis_lua/channel_state_lease_renew.lua
	channelStateLeaseRenewScriptSource string

	//go:embed internal/redis_lua/channel_state_lease_release.lua
	channelStateLeaseReleaseScriptSource string
)

// ChannelStateEventType is the type of a channel state event.
type ChannelStateEventType string

const (
	// ChannelStateOccupied is sent when a channel gets its first subscriber in the cluster.
	ChannelStateOccupied ChannelStateEventType = "occupied"
	// ChannelStateVacated is sent when a channel loses its last subscriber in the cluster.
	ChannelStateVacated ChannelStateEventType = "vacated"
)

// ChannelStateEvent is a channel state event delivered to a ChannelStateHandler.
type ChannelStateEvent struct {
	// Channel is the channel the event is about.
	Channel string
	// Type is ChannelStateOccupied or ChannelStateVacated.
	Type ChannelStateEventType
	// Time is when the event happened in the cluster.
	Time time.Time
}

// ChannelStateHandler delivers one batch of a partition, in order. A non-nil error
// makes the batch be delivered again after a backoff. The context is cancelled when
// the presence manager is closed or the partition moves to another node. The handler
// must return once ctx is done. When a partition moves to another node, its owner releases
// the lease only after a running handler has returned. A lease that expires instead, because
// its owner stalled or failed to renew it, can be taken while the old handler still runs: a
// handler that overruns ctx may deliver its batch beside the new owner, and a batch stalled
// past the lease TTL can reach the handler after later events of its channels. The backend
// must therefore reconcile by presence, not rely on the order of events across owners.
// Close releases the leases first and then waits for the running handlers, up to the context
// of Close: a handler that is still running after that may deliver its batch while another
// node takes the partition.
type ChannelStateHandler func(ctx context.Context, events []ChannelStateEvent) error

// RedisChannelStateConfig enables channel state events (occupied, vacated) in
// RedisPresenceManager. Zero values select the defaults.
//
// The partitions are divided among the nodes that run channel state, the workers of the prefix.
// A worker publishes itself in a set on the first Redis shard, and an entry expires after LeaseTTL unless it is
// renewed. Nodes without channel state are not counted, so a rolling enable on some nodes keeps every partition
// consumed while one worker runs. When a worker crashes, its partitions move to the survivors after about LeaseTTL.
// A worker that shuts down releases its partitions at once, but a survivor takes them at its next probe, up to
// LeaseTTL later. While the first shard cannot be reached the count is unknown: a worker keeps its partitions and
// takes the free ones until the count is back. The timing of the lease loop is in FORK.md, Channel state.
//
// Presence is not split across the partitions. The presence of an enabled channel is kept in its own
// keys, which a node without channel state does not read. Deploy the configuration to every node of
// the prefix before a client subscribes to an enabled channel. Otherwise the events and the presence
// of that channel are split between the two layouts, and a vacated event can go out while a client on
// another node is still subscribed.
//
// Membership uses the wall clock of each node. Clock skew larger than LeaseTTL delays the takeover of a
// crashed worker's partitions by about the skew, because a worker that is ahead of the others stays in
// their count after it crashed. A worker whose clock lags is left out of the count of the other workers while
// it runs. It takes a share of its own count, and the leases keep one consumer per partition.
//
// The lease check uses the wall clock. A backward step of the clock (NTP) can keep a lease
// believed held after Redis expired it, so two nodes may deliver one partition for about the
// size of the step. Delivery is at least once, so those events may repeat.
//
// The partition workers start with NewRedisPresenceManager and run until Close is called on the presence
// manager. Node.Shutdown closes the presence manager it was given. A manager that is built and never
// closed keeps its leases and calls Handler until the process exits.
type RedisChannelStateConfig struct {
	// Enabled reports whether channel state events are kept for the channel. Required. The answer
	// must not change for a channel while it has subscribers: keys are chosen per call, so toggling
	// it on live channels is not supported.
	Enabled func(channel string) bool
	// Handler receives the events. Required. An error keeps the batch in the stream, and the batch is
	// delivered again after a delay that grows. The redelivery comes at the first sweep tick after the
	// delay, so the gap is the delay plus up to one SweepInterval. So an event that always fails blocks
	// the delivery of its partition. The sweeps keep running between the attempts, but a failing batch runs one
	// sweep per delay, so under a persistent failure a sweep comes up to every 20 seconds, not every SweepInterval.
	// A full stream does not stop the sweeps either: a vacated event kept pending at the cap is retried by the next
	// sweep, so under a persistent failure that retry can wait as long as the delay. There is no dead-letter stream.
	Handler ChannelStateHandler
	// NumPartitions is the number of partitions that serialize channel state in Redis.
	// It must not change while channels are occupied. Default 128.
	NumPartitions int
	// VacatedEventDelay is how long a channel stays occupied after its last subscriber
	// left; a subscribe within the delay cancels the vacated event. Zero selects the default
	// 5s, so a configured zero delay must be passed as a negative value: that means no delay,
	// and the vacated event is sent by the next sweep. Default 5s.
	VacatedEventDelay time.Duration
	// MaxStreamLength caps the events waiting for delivery in one partition; new occupied events
	// are dropped when the cap is reached. Negative removes the cap. Default 8192.
	// A dropped occupied event is not sent again, and later subscribes of the channel send none
	// either while the channel stays occupied, so the application must reconcile the state from presence. A vacated event at the
	// cap is not dropped: it stays pending, and a later sweep sends it once the stream has room. While the stream stays at
	// the cap, each kept vacated event is checked once per SweepInterval, so a sweep of a partition with many kept channels
	// runs up to 10 pages of 100 checks, each with a write. See FORK.md, Channel state.
	// Dropped occupied events are counted in the events_dropped_count metric.
	MaxStreamLength int
	// SweepInterval is how often a partition is swept and its events are read. A value below 100ms is raised to
	// 100ms when the manager is created, and a warning is logged. Default 1s.
	// Every partition of a node sweeps on this interval, so Redis load grows with 1/SweepInterval.
	SweepInterval time.Duration
	// LeaseTTL is how long a partition lease lasts without renewal. A lease is renewed every LeaseTTL/5,
	// and a Redis call of the lease loop gives up after LeaseTTL/3, so a reply that is slower than that
	// gives up the shard for the cycle. A node that holds more than its share gives up one partition per
	// renewal, so a node that joins a worker holding every partition waits about half of NumPartitions
	// renewals for its share: 64 s with 128 partitions and the default 5s. A node with no free partition
	// probes for one after a delay that doubles from LeaseTTL/5 up to LeaseTTL, so a partition freed by a
	// rebalance can be taken up to LeaseTTL later than the renewals suggest.
	// Must be at least 5ms. Values below 500ms run the lease loop more often than every 100ms, with the
	// Redis calls of each renewal and heartbeat, so use them only in tests. Default 5s.
	LeaseTTL time.Duration
	// BatchSize is the maximum number of events in one Handler call. Default 100.
	BatchSize int
}

// presenceState keeps channel state of a RedisPresenceManager: the partition layout,
// the scripts and the partition worker.
type presenceState struct {
	manager           *RedisPresenceManager
	enabled           func(channel string) bool
	handler           ChannelStateHandler
	numPartitions     int
	partitionShards   []int
	partitionTags     []string
	workersKey        string
	workersShard      *RedisShard
	vacatedEventDelay time.Duration
	maxStreamLength   int
	sweepInterval     time.Duration
	leaseTTL          time.Duration
	batchSize         int
	expiredBatch      string
	metrics           *channelStateMetrics
	dropLoggedAt      atomic.Int64
	keptLoggedAt      atomic.Int64

	addScript     *rueidis.Lua
	remScript     *rueidis.Lua
	checkScript   *rueidis.Lua
	renewScript   *rueidis.Lua
	releaseScript *rueidis.Lua

	worker *channelStateWorker
}

func newPresenceState(m *RedisPresenceManager, config RedisChannelStateConfig) (*presenceState, error) {
	if config.Enabled == nil {
		return nil, errors.New("presence: channel state requires Enabled")
	}
	if config.Handler == nil {
		return nil, errors.New("presence: channel state requires Handler")
	}

	numPartitions := config.NumPartitions
	if numPartitions <= 0 {
		numPartitions = channelStateDefaultNumPartitions
	}

	vacatedEventDelay := config.VacatedEventDelay
	if vacatedEventDelay == 0 {
		vacatedEventDelay = channelStateDefaultVacatedEventDelay
	} else if vacatedEventDelay < 0 {
		vacatedEventDelay = 0
	}

	maxStreamLength := config.MaxStreamLength
	if maxStreamLength == 0 {
		maxStreamLength = channelStateDefaultMaxStreamLength
	} else if maxStreamLength < 0 {
		maxStreamLength = 0
	}

	sweepInterval := config.SweepInterval
	if sweepInterval <= 0 {
		sweepInterval = channelStateDefaultSweepInterval
	} else if sweepInterval < channelStateMinSweepInterval {
		m.node.logger.log(newLogEntry(LogLevelWarn, "channel state SweepInterval is below 100ms, using 100ms", map[string]any{
			"sweep_interval": sweepInterval.String(),
		}))
		sweepInterval = channelStateMinSweepInterval
	}

	leaseTTL := config.LeaseTTL
	if leaseTTL == 0 {
		leaseTTL = channelStateDefaultLeaseTTL
	}
	if leaseTTL < channelStateMinLeaseTTL {
		return nil, errors.New("presence: channel state LeaseTTL must be at least 5ms")
	}

	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = channelStateDefaultBatchSize
	}

	cm, err := newChannelStateMetrics(m.node.metrics)
	if err != nil {
		return nil, err
	}

	partitionShards := make([]int, numPartitions)
	partitionTags := make([]string, numPartitions)
	for p := range partitionShards {
		partitionShards[p] = consistentIndex("cs"+strconv.Itoa(p), len(m.shards))
		partitionTags[p] = m.config.Prefix + ".channel_state.{cs" + strconv.Itoa(p) + "}."
	}

	s := &presenceState{
		manager:           m,
		enabled:           config.Enabled,
		handler:           config.Handler,
		numPartitions:     numPartitions,
		partitionShards:   partitionShards,
		partitionTags:     partitionTags,
		workersKey:        m.config.Prefix + ".channel_state.workers",
		workersShard:      m.shards[0],
		vacatedEventDelay: vacatedEventDelay,
		maxStreamLength:   maxStreamLength,
		sweepInterval:     sweepInterval,
		leaseTTL:          leaseTTL,
		batchSize:         batchSize,
		expiredBatch:      strconv.Itoa(channelStateExpiredBatch),
		metrics:           cm,

		addScript:     rueidis.NewLuaScript(presenceStateAddScriptSource, rueidis.WithLoadSHA1(m.config.LoadSHA1)),
		remScript:     rueidis.NewLuaScript(presenceStateRemScriptSource, rueidis.WithLoadSHA1(m.config.LoadSHA1)),
		checkScript:   rueidis.NewLuaScript(presenceStateCheckScriptSource, rueidis.WithLoadSHA1(m.config.LoadSHA1)),
		renewScript:   rueidis.NewLuaScript(channelStateLeaseRenewScriptSource, rueidis.WithLoadSHA1(m.config.LoadSHA1)),
		releaseScript: rueidis.NewLuaScript(channelStateLeaseReleaseScriptSource, rueidis.WithLoadSHA1(m.config.LoadSHA1)),
	}
	s.worker = newChannelStateWorker(s, m.node)
	return s, nil
}

// startChannelState builds the channel state of the manager and starts its partition worker.
func (m *RedisPresenceManager) startChannelState(config RedisChannelStateConfig) error {
	state, err := newPresenceState(m, config)
	if err != nil {
		return err
	}
	m.state = state
	go state.worker.run()
	return nil
}

// stateEnabled reports whether the channel keeps channel state. A nil state, a manager without
// channel state, keeps no channel state, so the guards of the presence manager need no nil check.
func (s *presenceState) stateEnabled(channel string) bool {
	return s != nil && s.enabled(channel)
}

// partition returns the channel state partition of the channel.
func (s *presenceState) partition(channel string) int {
	return index(channel, s.numPartitions)
}

// shardOf returns the Redis shard that keeps the state of the partition.
func (s *presenceState) shardOf(p int) *RedisShard {
	return s.manager.shards[s.partitionShards[p]]
}

// partitionKey returns a key of the partition: all of them share the tag of the partition,
// so one script may touch the channel keys and the partition keys in Redis Cluster.
func (s *presenceState) partitionKey(p int, suffix string) channelID {
	return channelID(s.partitionTags[p] + suffix)
}

// presenceKey returns a presence key of the channel, tagged with the partition of the channel. The key
// lives under the channel state prefix, not under the presence prefix: presence keys of channels without
// channel state always start with prefix.presence., so no channel name can make them collide with the
// keys of a channel with channel state.
func (s *presenceState) presenceKey(channel string, kind string) channelID {
	return channelID(s.partitionTags[s.partition(channel)] + "presence." + kind + "." + channel)
}

func (s *presenceState) stateHashKey(channel string) channelID {
	return s.presenceKey(channel, "data")
}

func (s *presenceState) stateSetKey(channel string) channelID {
	return s.presenceKey(channel, "expire")
}

func (s *presenceState) stateUserSetKey(channel string) channelID {
	return s.presenceKey(channel, "user.expire")
}

func (s *presenceState) stateUserHashKey(channel string) channelID {
	return s.presenceKey(channel, "user.clients")
}

// addPresence adds the client and emits the occupied event when the channel had no subscribers.
func (s *presenceState) addPresence(sh *RedisShard, channel string, uid string, info *ClientInfo) error {
	keys, args, err := s.manager.addPresenceScriptKeysArgs(sh, channel, uid, info)
	if err != nil {
		return err
	}
	p := s.partition(channel)
	nowMs := time.Now().UnixMilli()
	keys = append(
		keys,
		string(s.partitionKey(p, "stream")),
		string(s.partitionKey(p, "pending")),
		string(s.partitionKey(p, "expire")),
	)
	args = append(
		args,
		channel,
		strconv.FormatInt(nowMs, 10),
		strconv.FormatInt(nowMs+s.manager.config.PresenceTTL.Milliseconds(), 10),
		strconv.Itoa(s.maxStreamLength),
	)
	result, err := s.addScript.Exec(context.Background(), sh.client, keys, args).AsInt64()
	if err != nil {
		return err
	}
	s.countResult(ChannelStateOccupied, result)
	return nil
}

// removePresence removes the client and arms the vacated event when the channel has no subscribers left.
func (s *presenceState) removePresence(sh *RedisShard, channel string, clientID string, userID string) error {
	keys, args, err := s.manager.removePresenceScriptKeysArgs(sh, channel, clientID, userID)
	if err != nil {
		return err
	}
	p := s.partition(channel)
	nowMs := time.Now().UnixMilli()
	keys = append(
		keys,
		string(s.partitionKey(p, "pending")),
		string(s.partitionKey(p, "expire")),
	)
	args = append(
		args,
		channel,
		strconv.FormatInt(nowMs/1000, 10),
		strconv.FormatInt(nowMs+s.vacatedEventDelay.Milliseconds(), 10),
		s.expiredBatch,
	)
	return s.remScript.Exec(context.Background(), sh.client, keys, args).Error()
}

// checkChannels runs the sweep check of the channels of one partition, in batches of
// channelStateSweepPageSize pipelined commands. A vacated event kept pending by a full stream is not an error:
// the script moves its pending entry one sweep interval ahead, so the next sweep retries it, and the channels due
// behind it are checked in the batches that follow.
func (s *presenceState) checkChannels(ctx context.Context, sh *RedisShard, p int, channels []string, nowMs int64) error {
	stream := string(s.partitionKey(p, "stream"))
	pending := string(s.partitionKey(p, "pending"))
	expire := string(s.partitionKey(p, "expire"))
	nowStr := strconv.FormatInt(nowMs, 10)
	nowSecStr := strconv.FormatInt(nowMs/1000, 10)
	fireStr := strconv.FormatInt(nowMs+s.vacatedEventDelay.Milliseconds(), 10)
	useHashFieldTTL := s.manager.useHashFieldTTLArg()
	maxLenStr := strconv.Itoa(s.maxStreamLength)
	ttlStr := strconv.FormatInt(s.manager.config.PresenceTTL.Milliseconds(), 10)
	sweepIntervalStr := strconv.FormatInt(s.sweepInterval.Milliseconds(), 10)

	for start := 0; start < len(channels); start += channelStateSweepPageSize {
		end := min(start+channelStateSweepPageSize, len(channels))
		execs := make([]rueidis.LuaExec, 0, end-start)
		for _, channel := range channels[start:end] {
			execs = append(execs, rueidis.LuaExec{
				Keys: []string{string(s.stateSetKey(channel)), string(s.stateHashKey(channel)), stream, pending, expire},
				Args: []string{channel, nowStr, nowSecStr, fireStr, useHashFieldTTL, maxLenStr, ttlStr, s.expiredBatch, sweepIntervalStr},
			})
		}
		for _, res := range s.checkScript.ExecMulti(ctx, sh.client, execs...) {
			result, err := res.AsInt64()
			if err != nil {
				return err
			}
			s.countResult(ChannelStateVacated, result)
		}
	}
	return nil
}

// countResult counts the outcome of a state script: 1 is an event written to the stream,
// 2 is an occupied event dropped because the stream is full. A vacated event kept pending
// at the cap is 3 and is not counted, since it is sent later, but it is logged because it
// stalls the sweep of its partition.
func (s *presenceState) countResult(eventType ChannelStateEventType, result int64) {
	switch result {
	case 1:
		s.metrics.incEmitted(eventType)
	case 2:
		s.metrics.incDropped(eventType)
		s.logDropped(eventType)
	case channelStateKeptPending:
		s.logKeptPending()
	}
}

// logDropped logs a dropped event at most once per minute. A full stream means the backend does not
// answer, so the log shows it without a line per dropped event.
func (s *presenceState) logDropped(eventType ChannelStateEventType) {
	if !s.allowLog(&s.dropLoggedAt) {
		return
	}
	s.manager.node.logger.log(newLogEntry(LogLevelWarn, "channel state event dropped: the stream of the partition is full", map[string]any{
		"type":              string(eventType),
		"max_stream_length": s.maxStreamLength,
	}))
}

// logKeptPending logs a vacated event kept pending at the stream cap at most once per minute. The event is retried
// after one sweep interval, so the log names the full stream as the cause.
func (s *presenceState) logKeptPending() {
	if !s.allowLog(&s.keptLoggedAt) {
		return
	}
	s.manager.node.logger.log(newLogEntry(LogLevelWarn, "channel state vacated event kept pending: the stream of the partition is full, the event is retried", map[string]any{
		"max_stream_length": s.maxStreamLength,
	}))
}

// allowLog reports whether a log line may be written now, at most one per minute for the given timestamp.
func (s *presenceState) allowLog(last *atomic.Int64) bool {
	now := time.Now().UnixNano()
	prev := last.Load()
	return now-prev >= int64(time.Minute) && last.CompareAndSwap(prev, now)
}

// close stops the partition workers and releases their leases. It returns early with
// the context error when the context is done first. A nil state has no workers.
func (s *presenceState) close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.worker.stop()
	select {
	case <-s.worker.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
