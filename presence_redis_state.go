package centrifuge

import (
	"context"
	"errors"
	"strconv"
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
	channelStateSweepPageSize            = 100
	channelStateSweepMaxPages            = 10
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
// must return once ctx is done: the lease loop waits for a running handler before it
// releases the partition.
type ChannelStateHandler func(ctx context.Context, events []ChannelStateEvent) error

// RedisChannelStateConfig enables channel state events (occupied, vacated) in
// RedisPresenceManager. Zero values select the defaults.
//
// When a node crashes, the partitions it consumed move to the surviving nodes only after
// the node registry drops the crashed node, about 7 to 16 seconds after its last
// publication. Events of those partitions are delayed by that much.
type RedisChannelStateConfig struct {
	// Enabled reports whether channel state events are kept for the channel. Required. The answer
	// must not change for a channel while it has subscribers: keys are chosen per call, so toggling
	// it on live channels is not supported.
	Enabled func(channel string) bool
	// Handler receives the events. Required.
	Handler ChannelStateHandler
	// NumPartitions is the number of partitions that serialize channel state in Redis.
	// It must not change while channels are occupied. Default 128.
	NumPartitions int
	// VacatedEventDelay is how long a channel stays occupied after its last subscriber
	// left; a subscribe within the delay cancels the vacated event. Zero selects the default
	// 5s, so a configured zero delay must be passed as a negative value: that means no delay,
	// and the vacated event is sent by the next sweep. Default 5s.
	VacatedEventDelay time.Duration
	// MaxStreamLength caps the events waiting for delivery in one partition; new events
	// are dropped when the cap is reached. Negative removes the cap. Default 8192.
	MaxStreamLength int
	// SweepInterval is how often a partition is swept and its events are read. Default 1s.
	SweepInterval time.Duration
	// LeaseTTL is how long a partition lease lasts without renewal; a lease is renewed
	// every LeaseTTL/5. Must be at least 5ms. Default 5s.
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
	vacatedEventDelay time.Duration
	maxStreamLength   int
	sweepInterval     time.Duration
	leaseTTL          time.Duration
	batchSize         int
	metrics           *channelStateMetrics

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

	cm, err := m.node.metrics.enableChannelStateMetrics()
	if err != nil {
		return nil, err
	}

	partitionShards := make([]int, numPartitions)
	for p := range partitionShards {
		partitionShards[p] = consistentIndex("cs"+strconv.Itoa(p), len(m.shards))
	}

	s := &presenceState{
		manager:           m,
		enabled:           config.Enabled,
		handler:           config.Handler,
		numPartitions:     numPartitions,
		partitionShards:   partitionShards,
		vacatedEventDelay: vacatedEventDelay,
		maxStreamLength:   maxStreamLength,
		sweepInterval:     sweepInterval,
		leaseTTL:          leaseTTL,
		batchSize:         batchSize,
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
	return channelID(s.manager.config.Prefix + ".channel_state.{cs" + strconv.Itoa(p) + "}." + suffix)
}

// presenceKey returns a presence key of the channel, tagged with the partition of the channel.
func (s *presenceState) presenceKey(channel string, kind string) channelID {
	return channelID(s.manager.config.Prefix + ".presence." + kind + ".{cs" + strconv.Itoa(s.partition(channel)) + "}." + channel)
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
	)
	return s.remScript.Exec(context.Background(), sh.client, keys, args).Error()
}

// checkChannels runs the sweep check of the channels of one partition, in batches of
// channelStateSweepPageSize pipelined commands.
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

	for start := 0; start < len(channels); start += channelStateSweepPageSize {
		end := min(start+channelStateSweepPageSize, len(channels))
		execs := make([]rueidis.LuaExec, 0, end-start)
		for _, channel := range channels[start:end] {
			execs = append(execs, rueidis.LuaExec{
				Keys: []string{string(s.stateSetKey(channel)), string(s.stateHashKey(channel)), stream, pending, expire},
				Args: []string{channel, nowStr, nowSecStr, fireStr, useHashFieldTTL, maxLenStr, ttlStr},
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
// 2 is an event dropped because the stream is full.
func (s *presenceState) countResult(eventType ChannelStateEventType, result int64) {
	switch result {
	case 1:
		s.metrics.incEmitted(eventType)
	case 2:
		s.metrics.incDropped(eventType)
	}
}

// close stops the partition workers and releases their leases. It returns early with
// the context error when the context is done first.
func (s *presenceState) close(ctx context.Context) error {
	s.worker.stop()
	select {
	case <-s.worker.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
