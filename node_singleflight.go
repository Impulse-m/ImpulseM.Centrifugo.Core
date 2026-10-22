package centrifuge

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"
)

// singleflightSharedCallTimeout bounds a call that singleflightDoContext shares between its callers. The call does
// not end with the context of the caller that started it, so it needs a bound of its own.
const singleflightSharedCallTimeout = 10 * time.Second

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

// singleflightDoContext is singleflightDo for a call that takes a context. The shared call runs with the values of
// ctx of the caller that started it, but without its cancellation, and is bounded by singleflightSharedCallTimeout.
// So the cancellation of one caller does not fail the others. Each caller returns ctx.Err() as soon as its own
// context is done, and otherwise the result of the shared call.
func singleflightDoContext[T any](ctx context.Context, group *singleflight.Group, merged prometheus.Counter, key string, fn func(context.Context) (T, error)) (T, error) {
	ch := group.DoChan(key, func() (any, error) {
		callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), singleflightSharedCallTimeout)
		defer cancel()
		return fn(callCtx)
	})
	select {
	case res := <-ch:
		if res.Shared {
			merged.Inc()
		}
		result, _ := res.Val.(T)
		return result, res.Err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}
