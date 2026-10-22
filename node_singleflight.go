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
	recordShared(merged, shared)
	result, _ := v.(T)
	return result, err
}

// recordShared counts a call that received the result of another in-flight call.
func recordShared(merged prometheus.Counter, shared bool) {
	if shared {
		merged.Inc()
	}
}

// singleflightPanic carries a panic of a shared call from the goroutine that runs the call to its callers.
type singleflightPanic struct {
	value any
}

// singleflightDoContext is singleflightDo for a call that takes a context. The shared call runs with the values of
// ctx of the caller that started it, but without its cancellation, and is bounded by singleflightSharedCallTimeout.
// So the cancellation of one caller does not fail the others. Each caller returns ctx.Err() as soon as its own
// context is done, and otherwise the result of the shared call. A panic of the shared call is raised again on the
// goroutine of each caller, as group.Do does, so a caller can recover it.
func singleflightDoContext[T any](ctx context.Context, group *singleflight.Group, merged prometheus.Counter, key string, fn func(context.Context) (T, error)) (T, error) {
	ch := group.DoChan(key, func() (any, error) {
		callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), singleflightSharedCallTimeout)
		defer cancel()
		return singleflightRecover(callCtx, fn)
	})
	select {
	case res := <-ch:
		recordShared(merged, res.Shared)
		if p, ok := res.Val.(singleflightPanic); ok {
			panic(p.value)
		}
		result, _ := res.Val.(T)
		return result, res.Err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// singleflightRecover runs fn and turns its panic into a singleflightPanic value. A panic that leaves the
// shared call unrecovered would stop the process, because singleflight raises it on a goroutine of its own.
func singleflightRecover[T any](ctx context.Context, fn func(context.Context) (T, error)) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			result = singleflightPanic{value: r}
			err = nil
		}
	}()
	return fn(ctx)
}
