package channel

import (
	"bufio"
	"strings"
	"sync"
	"testing"
	"time"
)

// serveDeadline bounds the loops below. They run on their own goroutine
// and could otherwise park the suite.
const serveDeadline = 5 * time.Second

// collector gathers what the library warned about. A test asserts on the
// text and on how often it was said.
type collector struct {
	mu    sync.Mutex
	lines []string
}

func (c *collector) warn(message string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, message)
}

func (c *collector) said() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...)
}

// runReadLoop drives one canned stream through a shepherd and fails at
// the deadline rather than hanging.
func runReadLoop(t *testing.T, shepherd *Shepherd, stream string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		shepherd.readLoop(bufio.NewReader(strings.NewReader(stream)))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(serveDeadline):
		t.Fatal("readLoop never returned")
	}
}

func testShepherd(warn func(string)) *Shepherd {
	return &Shepherd{out: newOutbox(outboxCapacity), handlers: newDispatch(), warn: warn}
}

// D3: an app must be able to call every method without asking whether it
// has a channel.
func TestAnInertHandleAcceptsEverythingAndDoesNothing(t *testing.T) {
	shepherd := inert("", func(string) {})
	if shepherd.Active() {
		t.Fatal("an inert handle reported itself active")
	}
	shepherd.OnAction("gc", func(Action) string { return "ok" })
	shepherd.OnShutdown(func() {})
	shepherd.Metric("rps", 42)
	if err := shepherd.Ready(); err != nil {
		t.Fatalf("an inert Ready returned %v", err)
	}
	if got := shepherd.DroppedMetrics(); got != 0 {
		t.Fatalf("DroppedMetrics is %d, want 0", got)
	}
	if got := shepherd.Version(); got != "" {
		t.Fatalf("Version is %q, want empty", got)
	}
}

func TestAHandleStopsBeingActiveOnceTheChannelCloses(t *testing.T) {
	shepherd := testShepherd(func(string) {})
	if !shepherd.Active() {
		t.Fatal("a fresh channel did not read as live")
	}
	shepherd.out.close()
	if shepherd.Active() {
		t.Fatal("a handle whose shepherd went away still reads as live")
	}
}

// An author reading this line is deciding which field to set.
func TestTheNoChannelAdviceNamesEveryFieldThatOpensOne(t *testing.T) {
	for _, field := range []string{"channel = true", "wait_ready", "shutdown_with_message"} {
		if !strings.Contains(noChannelAdvice, field) {
			t.Fatalf("the advice does not mention %s", field)
		}
	}
}

// D5 makes this warning the only thing between a missing handler and a
// kill_timeout kill.
func TestTheUnhandledShutdownWarningNamesTheMethodToCall(t *testing.T) {
	for _, wanted := range []string{"OnShutdown", "kill_timeout"} {
		if !strings.Contains(unhandledShutdownAdvice, wanted) {
			t.Fatalf("the warning does not mention %s", wanted)
		}
	}
}

func TestTheNoChannelWarningFiresOnlyUnderShep(t *testing.T) {
	outside := &collector{}
	start(fakeEnv(map[string]string{}), outside.warn)
	if said := outside.said(); len(said) != 0 {
		t.Fatalf("an app running outside shep was warned: %v", said)
	}

	under := &collector{}
	start(fakeEnv(map[string]string{nameVar: "web"}), under.warn)
	said := under.said()
	if len(said) != 1 || !strings.Contains(said[0], "channel = true") {
		t.Fatalf("an app under shep with no channel was told %v", said)
	}
}

// D7: one descriptor has one owner. A second call hands back the first
// handle and says why. The only test here that calls the singleton,
// which is process-global and answers once.
func TestServeHandsBackOneHandleAndWarnsOnce(t *testing.T) {
	warnings := &collector{}
	env := fakeEnv(map[string]string{})

	first := serveShared(env, warnings.warn)
	second := serveShared(env, warnings.warn)
	third := serveShared(env, warnings.warn)

	if first != second || second != third {
		t.Fatalf("three calls handed back %p, %p and %p", first, second, third)
	}
	said := warnings.said()
	if len(said) != 1 {
		t.Fatalf("three calls produced %d warnings: %v", len(said), said)
	}
	for _, wanted := range []string{"more than once", "cannot be opened twice"} {
		if !strings.Contains(said[0], wanted) {
			t.Fatalf("the warning does not say %q: %s", wanted, said[0])
		}
	}
}

func TestTwoMalformedLinesWarnOnceAndTheLoopKeepsGoing(t *testing.T) {
	warnings := &collector{}
	shepherd := testShepherd(warnings.warn)
	stopped := make(chan struct{})
	shepherd.OnShutdown(func() { close(stopped) })

	runReadLoop(t, shepherd, "not json\nalso not json\n{\"kind\":\"shutdown\"}\n")

	select {
	case <-stopped:
	default:
		t.Fatal("the shutdown after two bad lines was never reached")
	}
	if said := warnings.said(); len(said) != 1 {
		t.Fatalf("two bad lines produced %d warnings: %v", len(said), said)
	}
}

// End of stream has to close the outbox. Otherwise the writer goroutine
// parks on a queue nobody will add to.
func TestEndOfStreamClosesTheOutbox(t *testing.T) {
	shepherd := testShepherd(func(string) {})
	runReadLoop(t, shepherd, "")
	if !shepherd.out.isClosed() {
		t.Fatal("readLoop returned without closing the outbox")
	}
}

func TestAnActionsReplyReachesTheOutboxCarryingItsID(t *testing.T) {
	shepherd := testShepherd(func(string) {})
	shepherd.OnAction("gc", func(a Action) string { return "collected " + strings.Join(a.Fields(), ",") })

	runReadLoop(t, shepherd, "{\"kind\":\"action\",\"name\":\"gc\",\"params\":\"now please\",\"id\":7}\n")

	reply := <-shepherd.out.messages
	if reply.Kind != KindActionReply || *reply.Action != "gc" {
		t.Fatalf("reply is %+v", reply)
	}
	if *reply.Body != "collected now,please" {
		t.Fatalf("body is %q", *reply.Body)
	}
	if reply.ID == nil || *reply.ID != 7 {
		t.Fatalf("the id was not echoed: %+v", reply.ID)
	}
}

func TestAnUnknownActionStillGetsAReplyCarryingItsID(t *testing.T) {
	shepherd := testShepherd(func(string) {})

	runReadLoop(t, shepherd, "{\"kind\":\"action\",\"name\":\"typo\",\"id\":8}\n")

	reply := <-shepherd.out.messages
	if *reply.Body != "unknown action: typo" {
		t.Fatalf("body is %q", *reply.Body)
	}
	if reply.ID == nil || *reply.ID != 8 {
		t.Fatalf("the id was not echoed: %+v", reply.ID)
	}
}

// Registering from inside a handler is what a reload action does. This
// runs the whole read loop, not just the registry. It covers the
// ordering the reader uses.
func TestAHandlerThatRegistersAHandlerDoesNotDeadlockTheReader(t *testing.T) {
	shepherd := testShepherd(func(string) {})
	shepherd.OnAction("reload", func(Action) string {
		shepherd.OnAction("late", func(Action) string { return "late ok" })
		return "reloaded"
	})

	runReadLoop(t, shepherd,
		"{\"kind\":\"action\",\"name\":\"reload\",\"id\":1}\n"+
			"{\"kind\":\"action\",\"name\":\"late\",\"id\":2}\n")

	first := <-shepherd.out.messages
	second := <-shepherd.out.messages
	if *first.Body != "reloaded" {
		t.Fatalf("the first body is %q", *first.Body)
	}
	if *second.Body != "late ok" {
		t.Fatalf("the second body is %q, so the late handler was not reachable", *second.Body)
	}
}
