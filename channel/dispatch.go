package channel

import (
	"fmt"
	"sync"
)

// dispatch holds the registered handlers.
//
// Resolving takes the lock and running does not. A handler that
// registers a handler would otherwise deadlock on the same lock.
type dispatch struct {
	mu       sync.RWMutex
	actions  map[string]func(Action) string
	shutdown func()
}

func newDispatch() *dispatch {
	return &dispatch{actions: make(map[string]func(Action) string)}
}

// registerAction replaces any handler already registered under name.
func (d *dispatch) registerAction(name string, handler func(Action) string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.actions[name] = handler
}

// registerShutdown replaces any handler already registered.
func (d *dispatch) registerShutdown(handler func()) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.shutdown = handler
}

// resolveAction returns the handler registered under name, if any.
func (d *dispatch) resolveAction(name string) (func(Action) string, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	handler, registered := d.actions[name]
	return handler, registered
}

// resolveShutdown returns the shutdown handler, if one is registered.
func (d *dispatch) resolveShutdown() (func(), bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.shutdown, d.shutdown != nil
}

// replyBody runs handler and returns what to send back.
//
// An unregistered name, a panic and a handler calling runtime.Goexit
// all produce a body. recover cannot catch a Goexit, so it unwinds a
// goroutine this call waits for.
func replyBody(handler func(Action) string, registered bool, action Action) string {
	if !registered {
		return "unknown action: " + action.Name
	}
	body := "action handler ended its goroutine"
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if recovered := recover(); recovered != nil {
				body = fmt.Sprintf("action handler failed: %v", recovered)
			}
		}()
		body = handler(action)
	}()
	<-done
	return body
}

// runShutdown runs handler and reports what went wrong, if anything.
//
// The unwind from a panic or a runtime.Goexit would otherwise end the
// read loop. The app only wanted to stop gracefully. The handler runs
// on a goroutine this call waits for.
func runShutdown(handler func()) (problem string, failed bool) {
	problem, failed = "shutdown handler ended its goroutine", true
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if recovered := recover(); recovered != nil {
				problem, failed = fmt.Sprintf("shutdown handler panicked: %v", recovered), true
			}
		}()
		handler()
		problem, failed = "", false
	}()
	<-done
	return problem, failed
}
