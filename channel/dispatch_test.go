package channel

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

// dispatchDeadline bounds the deadlock test below. Without it a
// regression parks the whole suite instead of failing it.
const dispatchDeadline = 5 * time.Second

func TestARegisteredActionRunsItsHandler(t *testing.T) {
	registry := newDispatch()
	registry.registerAction("gc", func(a Action) string {
		return a.Name + " ran with " + strings.Join(a.Fields(), ",")
	})

	handler, registered := registry.resolveAction("gc")
	body := replyBody(handler, registered, Action{Name: "gc", Params: ptr("now please")})
	if body != "gc ran with now,please" {
		t.Fatalf("body is %q", body)
	}
}

// The contract calls this out. Without a reply the operator waits
// out the whole action_timeout for a typo.
func TestAnUnregisteredActionStillGetsAReply(t *testing.T) {
	registry := newDispatch()
	handler, registered := registry.resolveAction("reload-config")
	body := replyBody(handler, registered, Action{Name: "reload-config"})
	if body != "unknown action: reload-config" {
		t.Fatalf("body is %q", body)
	}
}

// An app that panics should not cost the operator a timeout as well.
func TestAPanickingHandlerRepliesWithTheMessage(t *testing.T) {
	registry := newDispatch()
	registry.registerAction("boom", func(Action) string { panic("no such state") })

	handler, registered := registry.resolveAction("boom")
	body := replyBody(handler, registered, Action{Name: "boom"})
	if body != "action handler failed: no such state" {
		t.Fatalf("body is %q", body)
	}
}

func TestAShutdownHandlerRunsAndAPanicIsReported(t *testing.T) {
	registry := newDispatch()
	if _, registered := registry.resolveShutdown(); registered {
		t.Fatal("an empty registry reported a shutdown handler")
	}

	ran := false
	registry.registerShutdown(func() { ran = true })
	handler, registered := registry.resolveShutdown()
	if !registered {
		t.Fatal("the registered shutdown handler was not found")
	}
	if _, failed := runShutdown(handler); failed {
		t.Fatal("a working handler was reported as failed")
	}
	if !ran {
		t.Fatal("the shutdown handler never ran")
	}

	registry.registerShutdown(func() { panic("no such state") })
	handler, _ = registry.resolveShutdown()
	problem, failed := runShutdown(handler)
	if !failed || problem != "shutdown handler panicked: no such state" {
		t.Fatalf("a panicking handler reported %q, failed=%v", problem, failed)
	}
}

// recover cannot catch a runtime.Goexit, so neither handler runs on the
// caller's goroutine. The operator still gets an answer.
func TestHandlersThatEndTheirGoroutineStillProduceAnAnswer(t *testing.T) {
	registry := newDispatch()
	registry.registerAction("leave", func(Action) string {
		runtime.Goexit()
		return "never reached"
	})

	handler, registered := registry.resolveAction("leave")
	if body := replyBody(handler, registered, Action{Name: "leave"}); body != "action handler ended its goroutine" {
		t.Fatalf("body is %q", body)
	}

	registry.registerShutdown(func() { runtime.Goexit() })
	shutdown, _ := registry.resolveShutdown()
	problem, failed := runShutdown(shutdown)
	if !failed || problem != "shutdown handler ended its goroutine" {
		t.Fatalf("a shutdown handler that left reported %q, failed=%v", problem, failed)
	}
}

// A reload action that swaps its own handlers is ordinary. Holding the
// registry's lock across the handler would deadlock on exactly that.
func TestAHandlerThatRegistersAHandlerDoesNotDeadlock(t *testing.T) {
	registry := newDispatch()
	registry.registerAction("reload", func(Action) string {
		registry.registerAction("late", func(Action) string { return "late ok" })
		return "reloaded"
	})

	done := make(chan string, 1)
	go func() {
		handler, registered := registry.resolveAction("reload")
		done <- replyBody(handler, registered, Action{Name: "reload"})
	}()

	select {
	case body := <-done:
		if body != "reloaded" {
			t.Fatalf("body is %q", body)
		}
	case <-time.After(dispatchDeadline):
		t.Fatal("a handler that registers a handler deadlocked the caller")
	}

	handler, registered := registry.resolveAction("late")
	if body := replyBody(handler, registered, Action{Name: "late"}); body != "late ok" {
		t.Fatalf("the handler registered inside a handler was not reachable: %q", body)
	}
}
