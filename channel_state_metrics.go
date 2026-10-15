package centrifuge

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// channelStateMetrics are the metrics of channel state events. A node registers them the
// first time a presence manager enables channel state, so a node without channel state
// exports no channel state series.
type channelStateMetrics struct {
	eventsEmitted   *prometheus.CounterVec
	eventsDropped   *prometheus.CounterVec
	eventsDelivered prometheus.Counter
	deliveryErrors  prometheus.Counter
	sweepErrors     prometheus.Counter
	partitionsOwned prometheus.Gauge
}

func newChannelStateMetrics(namespace string) *channelStateMetrics {
	return &channelStateMetrics{
		eventsEmitted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "channel_state",
			Name:      "events_emitted_count",
			Help:      "Number of channel state events written to a stream by this node.",
		}, []string{"type"}),
		eventsDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "channel_state",
			Name:      "events_dropped_count",
			Help:      "Number of channel state events dropped because the stream of the partition was full.",
		}, []string{"type"}),
		eventsDelivered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "channel_state",
			Name:      "events_delivered_count",
			Help:      "Number of channel state events acknowledged by the handler.",
		}),
		deliveryErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "channel_state",
			Name:      "delivery_errors_count",
			Help:      "Number of channel state handler calls that returned an error.",
		}),
		sweepErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "channel_state",
			Name:      "sweep_errors_count",
			Help:      "Number of Redis errors of the channel state sweep and lease loops.",
		}),
		partitionsOwned: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "channel_state",
			Name:      "partitions_owned",
			Help:      "Number of channel state partitions this node consumes.",
		}),
	}
}

func (cm *channelStateMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		cm.eventsEmitted,
		cm.eventsDropped,
		cm.eventsDelivered,
		cm.deliveryErrors,
		cm.sweepErrors,
		cm.partitionsOwned,
	}
}

// enableChannelStateMetrics returns the channel state metrics of the node and registers
// them on the first call. It is called by the presence managers that enable channel state.
func (m *metrics) enableChannelStateMetrics() (*channelStateMetrics, error) {
	if m == nil {
		return nil, nil
	}
	if m.channelState != nil {
		return m.channelState, nil
	}
	cm := newChannelStateMetrics(m.namespace)
	var alreadyRegistered prometheus.AlreadyRegisteredError
	for _, collector := range cm.collectors() {
		if err := m.registerer.Register(collector); err != nil && !errors.As(err, &alreadyRegistered) {
			return nil, err
		}
	}
	m.channelState = cm
	return cm, nil
}

// The metric helpers are nil safe: a node without metrics keeps working.

func (cm *channelStateMetrics) incEmitted(eventType ChannelStateEventType) {
	if cm == nil {
		return
	}
	cm.eventsEmitted.WithLabelValues(string(eventType)).Inc()
}

func (cm *channelStateMetrics) incDropped(eventType ChannelStateEventType) {
	if cm == nil {
		return
	}
	cm.eventsDropped.WithLabelValues(string(eventType)).Inc()
}

func (cm *channelStateMetrics) addDelivered(count int) {
	if cm == nil {
		return
	}
	cm.eventsDelivered.Add(float64(count))
}

func (cm *channelStateMetrics) incDeliveryErrors() {
	if cm == nil {
		return
	}
	cm.deliveryErrors.Inc()
}

func (cm *channelStateMetrics) incSweepErrors() {
	if cm == nil {
		return
	}
	cm.sweepErrors.Inc()
}

func (cm *channelStateMetrics) setPartitionsOwned(count int) {
	if cm == nil {
		return
	}
	cm.partitionsOwned.Set(float64(count))
}
