package centrifuge

import (
	"context"
	"sync"
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

// singleflightGroup shares the calls of singleflightDoContext between concurrent callers with the same key. Unlike
// singleflight.Group it counts the callers that wait for each call. When the last of them leaves, the call is
// cancelled, so the broker is not read for a result that nobody waits for. The zero value is ready to use.
type singleflightGroup struct {
	mu    sync.Mutex
	calls map[string]*singleflightCall
}

// singleflightCall is one shared call of a singleflightGroup. waiters and the entry of the call in the map are
// guarded by singleflightGroup.mu. val and err are written before done is closed and read only after that.
type singleflightCall struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	val     any
	err     error
}

// singleflightDoContext is singleflightDo for a call that takes a context. The shared call runs with the values of
// ctx of the caller that started it, but without its cancellation, and is bounded by singleflightSharedCallTimeout.
// So the cancellation of one caller does not fail the others. Each caller returns ctx.Err() as soon as its own
// context is done, and otherwise the result of the shared call. When the last caller leaves before the call ends,
// the call is cancelled and the next caller with the key starts a new one. A panic of the shared call is raised
// again on the goroutine of each caller, as group.Do does, so a caller can recover it.
func singleflightDoContext[T any](ctx context.Context, group *singleflightGroup, merged prometheus.Counter, key string, fn func(context.Context) (T, error)) (T, error) {
	group.mu.Lock()
	if group.calls == nil {
		group.calls = make(map[string]*singleflightCall)
	}
	call, shared := group.calls[key]
	if !shared {
		callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), singleflightSharedCallTimeout)
		call = &singleflightCall{done: make(chan struct{}), cancel: cancel}
		group.calls[key] = call
		go group.run(key, call, func() (any, error) {
			return singleflightRecover(callCtx, fn)
		})
	}
	call.waiters++
	group.mu.Unlock()

	select {
	case <-call.done:
		recordShared(merged, shared)
		if p, ok := call.val.(singleflightPanic); ok {
			panic(p.value)
		}
		result, _ := call.val.(T)
		return result, call.err
	case <-ctx.Done():
		group.leave(key, call)
		var zero T
		return zero, ctx.Err()
	}
}

// run runs the shared call of key and publishes its result to the callers that wait for it.
func (g *singleflightGroup) run(key string, call *singleflightCall, fn func() (any, error)) {
	defer call.cancel()
	call.val, call.err = fn()
	g.mu.Lock()
	if g.calls[key] == call {
		delete(g.calls, key)
	}
	g.mu.Unlock()
	close(call.done)
}

// leave removes one waiting caller from call. When no caller waits any more, the call is removed from the group,
// so a new caller does not join it, and it is cancelled.
func (g *singleflightGroup) leave(key string, call *singleflightCall) {
	g.mu.Lock()
	defer g.mu.Unlock()
	call.waiters--
	if call.waiters > 0 {
		return
	}
	if g.calls[key] == call {
		delete(g.calls, key)
	}
	call.cancel()
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
