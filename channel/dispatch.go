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
// An unregistered name and a panicking handler both produce a body.
// Silence from either looks like an app thinking hard about a slow
// action.
func replyBody(handler func(Action) string, registered bool, action Action) (body string) {
	if !registered {
		return "unknown action: " + action.Name
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			body = fmt.Sprintf("action handler failed: %v", recovered)
		}
	}()
	return handler(action)
}

// runShutdown runs handler and reports whether it panicked.
//
// An unwind reaching the reader goroutine would take the process down.
// The app only wanted to stop gracefully.
func runShutdown(handler func()) (message string, failed bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			message = fmt.Sprint(recovered)
			failed = true
		}
	}()
	handler()
	return "", false
}
