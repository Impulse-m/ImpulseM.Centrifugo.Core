package centrifuge

import (
	"errors"
)

// errCallTimeout is the outcome of a call that did not return in time.
var errCallTimeout = errors.New("call did not return in time")

// errCallGoexit is the outcome of a call whose function ended its goroutine with runtime.Goexit. Such a call returns
// no result, so its caller gets this error instead of waiting for the timeout.
var errCallGoexit = errors.New("call ended without a result")

// callResult is the outcome of a call that runs on its own goroutine.
type callResult[T any] struct {
	value T
	err   error
}

// startCall runs fn on its own goroutine and returns the channel of its outcome. The channel is buffered, so
// a call that returns after the caller stopped waiting for it does not block.
func startCall[T any](fn func() (T, error)) <-chan callResult[T] {
	ch := make(chan callResult[T], 1)
	go func() {
		exited := true
		defer func() {
			if exited {
				ch <- callResult[T]{err: errCallGoexit}
			}
		}()
		value, err := fn()
		exited = false
		ch <- callResult[T]{value: value, err: err}
	}()
	return ch
}

// awaitCall waits for the outcome of a call until expired is closed. A result that is already there wins over
// the expiry. ok is false when the call did not return in time: rueidis queues a call behind a connection that
// is being dialed, and that wait does not end with the context of the call, so the lease loop does not wait for
// a call without a bound of its own.
func awaitCall[T any](ch <-chan callResult[T], expired <-chan struct{}) (res callResult[T], ok bool) {
	select {
	case res = <-ch:
		return res, true
	default:
	}
	select {
	case res = <-ch:
		return res, true
	case <-expired:
		select {
		case res = <-ch:
			return res, true
		default:
			return callResult[T]{err: errCallTimeout}, false
		}
	}
}
