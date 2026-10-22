package centrifuge

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"
)

// singleflightSharedCallTimeout bounds a call that singleflightDoContext shares between its callers. The call does
// not end with the context of the caller that started it, so it needs a bound of its own.
const singleflightSharedCallTimeout = 10 * time.Second

// errSingleflightGoexit is the error of a shared call whose function ended its goroutine with runtime.Goexit. Such a
// call returns no result, so each of its callers gets this error.
var errSingleflightGoexit = errors.New("singleflight: shared call ended without a result")

// singleflightDo runs fn once for concurrent callers of group with the same key and returns its result to
// all of them. A call that received the result of another in-flight call increments merged. The result is
// returned together with the error, as fn returned them.
func singleflightDo[T any](group *singleflight.Group, merged prometheus.Counter, key string, fn func() (T, error)) (T, error) {
	v, err, shared := group.Do(key, func() (any, error) {
		return fn()
	})
	return singleflightResult[T](v, err, shared, merged)
}

// singleflightResult returns the result of a shared call to one of its callers. A call that received the result of
// another in-flight call increments merged. A panic of the shared call is raised again here.
func singleflightResult[T any](value any, err error, shared bool, merged prometheus.Counter) (T, error) {
	if shared {
		merged.Inc()
	}
	if p, ok := value.(singleflightPanic); ok {
		panic(p.value)
	}
	result, _ := value.(T)
	return result, err
}

// singleflightPanic carries a panic of a shared call from the goroutine that runs the call to its callers.
type singleflightPanic struct {
	value any
}

// singleflightJoinHook is a test seam. When it is set, singleflightDoContext calls it with the key of each caller
// once the caller is registered in the group, so a test knows when a follower has joined a shared call. It stays nil
// in production.
var singleflightJoinHook atomic.Pointer[func(key string)]

// singleflightDoContext is singleflightDo for a call that takes a context. The shared call runs with the values of
// ctx of the caller that started it, but without its cancellation, and is bounded by singleflightSharedCallTimeout.
// So the cancellation of one caller does not fail the others, and a call that all its callers left still runs until
// its bound. Each caller returns ctx.Err() as soon as its own context is done, and otherwise the result of the shared
// call. A panic of the shared call is raised again on the goroutine of each caller, as group.Do does, so a caller can
// recover it.
func singleflightDoContext[T any](ctx context.Context, group *singleflight.Group, merged prometheus.Counter, key string, fn func(context.Context) (T, error)) (T, error) {
	resultCh := group.DoChan(key, func() (any, error) {
		callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), singleflightSharedCallTimeout)
		defer cancel()
		return singleflightRun(callCtx, fn)
	})
	if hook := singleflightJoinHook.Load(); hook != nil {
		(*hook)(key)
	}
	select {
	case res := <-resultCh:
		return singleflightResult[T](res.Val, res.Err, res.Shared, merged)
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// singleflightRun runs fn on a goroutine of its own and returns its outcome. A runtime.Goexit in fn ends that
// goroutine without an outcome, so the goroutine reports errSingleflightGoexit instead. Without this, the group would
// never answer its callers, which wait for their own contexts to end.
func singleflightRun[T any](ctx context.Context, fn func(context.Context) (T, error)) (any, error) {
	outcome := make(chan callResult[any], 1)
	go func() {
		exited := true
		defer func() {
			if exited {
				outcome <- callResult[any]{err: errSingleflightGoexit}
			}
		}()
		result, err := singleflightRecover(ctx, fn)
		exited = false
		outcome <- callResult[any]{value: result, err: err}
	}()
	res := <-outcome
	return res.value, res.err
}

// singleflightRecover runs fn and turns its panic into a singleflightPanic value. A panic that leaves the
// shared call unrecovered would stop the process, because the shared call runs on a goroutine of its own.
func singleflightRecover[T any](ctx context.Context, fn func(context.Context) (T, error)) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			result = singleflightPanic{value: r}
			err = nil
		}
	}()
	return fn(ctx)
}
