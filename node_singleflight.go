package centrifuge

import (
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"
)

// singleflightDo runs fn once for concurrent callers of group with the same key and returns its result to
// all of them. A call that received the result of another in-flight call increments merged. The result is
// returned together with the error, as fn returned them.
func singleflightDo[T any](group *singleflight.Group, merged prometheus.Counter, key string, fn func() (T, error)) (T, error) {
	v, err, shared := group.Do(key, func() (any, error) {
		return fn()
	})
	if shared {
		merged.Inc()
	}
	result, _ := v.(T)
	return result, err
}
