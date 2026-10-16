package centrifuge

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// channelStateMetrics are the metrics of channel state events. A presence manager registers them
// when it enables channel state, so a node without channel state exports no channel state series.
type channelStateMetrics struct {
	eventsEmitted   *prometheus.CounterVec
	eventsDropped   *prometheus.CounterVec
	eventsDelivered prometheus.Counter
	deliveryErrors  prometheus.Counter
	sweepErrors     prometheus.Counter
	partitionsOwned prometheus.Gauge
}

// newChannelStateMetrics registers the channel state metrics on the registerer of the node. A registerer
// that already holds them, as when a second node in the process shares it, returns the registered
// collectors, so both presence managers count into the same series. The partitions owned gauge is set
// by each presence manager, so with several managers on one registerer it shows the last value set.
func newChannelStateMetrics(m *metrics) (*channelStateMetrics, error) {
	if m == nil {
		return nil, nil
	}
	registerer := prometheus.DefaultRegisterer
	if m.config.RegistererGatherer != nil {
		registerer = m.config.RegistererGatherer
	}
	namespace := getMetricsNamespace(m.config)

	cm := &channelStateMetrics{}
	var err error
	cm.eventsEmitted, err = registerChannelStateCollector(registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "channel_state",
		Name:      "events_emitted_count",
		Help:      "Number of channel state events written to a stream by this node.",
	}, []string{"type"}))
	if err != nil {
		return nil, err
	}
	cm.eventsDropped, err = registerChannelStateCollector(registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "channel_state",
		Name:      "events_dropped_count",
		Help:      "Number of channel state events dropped because the stream of the partition was full.",
	}, []string{"type"}))
	if err != nil {
		return nil, err
	}
	cm.eventsDelivered, err = registerChannelStateCollector(registerer, prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "channel_state",
		Name:      "events_delivered_count",
		Help:      "Number of channel state events acknowledged by the handler.",
	}))
	if err != nil {
		return nil, err
	}
	cm.deliveryErrors, err = registerChannelStateCollector(registerer, prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "channel_state",
		Name:      "delivery_errors_count",
		Help:      "Number of channel state handler calls that returned an error.",
	}))
	if err != nil {
		return nil, err
	}
	cm.sweepErrors, err = registerChannelStateCollector(registerer, prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "channel_state",
		Name:      "sweep_errors_count",
		Help:      "Number of Redis errors of the channel state sweep and lease loops.",
	}))
	if err != nil {
		return nil, err
	}
	cm.partitionsOwned, err = registerChannelStateCollector(registerer, prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "channel_state",
		Name:      "partitions_owned",
		Help:      "Number of channel state partitions this node consumes.",
	}))
	if err != nil {
		return nil, err
	}
	return cm, nil
}

// registerChannelStateCollector registers the collector. When the registerer already holds a collector
// of the same description, that collector is returned instead.
func registerChannelStateCollector[T prometheus.Collector](registerer prometheus.Registerer, collector T) (T, error) {
	err := registerer.Register(collector)
	if err == nil {
		return collector, nil
	}
	var alreadyRegistered prometheus.AlreadyRegisteredError
	if errors.As(err, &alreadyRegistered) {
		if existing, ok := alreadyRegistered.ExistingCollector.(T); ok {
			return existing, nil
		}
	}
	return collector, err
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
