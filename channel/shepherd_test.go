package channel

import (
	"bufio"
	"errors"
	"io"
	"runtime"
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

// takeReply waits for one queued message. It fails at the deadline
// rather than parking the suite on a queue nothing will fill.
func takeReply(t *testing.T, shepherd *Shepherd) ChildMessage {
	t.Helper()
	select {
	case reply := <-shepherd.out.messages:
		return reply
	case <-time.After(serveDeadline):
		t.Fatal("no reply reached the outbox")
		return ChildMessage{}
	}
}

func testShepherd(warn func(string)) *Shepherd {
	return &Shepherd{out: newOutbox(outboxCapacity), handlers: newDispatch(), warn: warn}
}

// resetServe drops the process-global state Serve answers from.
//
// The same shape as releaseChannel, for the same reason. Go runs a
// package's tests in one process. Production never resets it, since a
// process has one channel and Serve answers about it once.
func resetServe() {
	serveOnce = sync.Once{}
	served = nil
	serveCalls.Store(0)
}

// D3: an app calls every method without asking whether it has a channel.
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

// A descriptor that will not open is a broken environment, not an absent
// channel. It is loud and still hands back a handle. This is the only
// inert handle that carries a version stamp.
func TestADescriptorThatWillNotOpenWarnsAndKeepsTheStamp(t *testing.T) {
	warnings := &collector{}
	handle := start(fakeEnv(map[string]string{
		FDVar:      "not-a-number",
		VersionVar: "99",
		nameVar:    "web",
	}), warnings.warn)

	if handle.Active() {
		t.Fatal("a handle over a descriptor that will not open reads as live")
	}
	if handle.Version() != "99" {
		t.Fatalf("Version is %q, want %q", handle.Version(), "99")
	}
	if err := handle.Ready(); err != nil {
		t.Fatalf("Ready on an inert handle returned %v", err)
	}

	said := warnings.said()
	if len(said) != 1 {
		t.Fatalf("a broken descriptor produced %d warnings: %v", len(said), said)
	}
	for _, wanted := range []string{FDVar, "not-a-number", "continuing without a channel"} {
		if !strings.Contains(said[0], wanted) {
			t.Fatalf("the warning does not say %q: %s", wanted, said[0])
		}
	}
}

// D7: one descriptor has one owner. A second call hands back the first
// handle and says why. The only test here that calls the singleton,
// which is process-global and answers once.
func TestServeHandsBackOneHandleAndWarnsOnce(t *testing.T) {
	resetServe()
	t.Cleanup(resetServe)
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

	reply := takeReply(t, shepherd)
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

	reply := takeReply(t, shepherd)
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

	first, second := takeReply(t, shepherd), takeReply(t, shepherd)
	if *first.Body != "reloaded" {
		t.Fatalf("the first body is %q", *first.Body)
	}
	if *second.Body != "late ok" {
		t.Fatalf("the second body is %q, so the late handler was not reachable", *second.Body)
	}
}

// failWriter refuses every write, which is what a socket does once the
// far end has gone.
type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

// errTransport is a broken connection rather than a shepherd that closed
// its end.
var errTransport = errors.New("connection reset by peer")

// errReader fails every read.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errTransport }

// An operator greps for one prefix. A reader that stopped answering has
// to be findable that way. End of stream has to stay quiet.
func TestAReadFailureWarnsAndACleanEndOfStreamDoesNot(t *testing.T) {
	broken := &collector{}
	shepherd := testShepherd(broken.warn)
	done := make(chan struct{})
	go func() {
		defer close(done)
		shepherd.readLoop(bufio.NewReader(errReader{}))
	}()
	select {
	case <-done:
	case <-time.After(serveDeadline):
		t.Fatal("readLoop never returned")
	}

	said := broken.said()
	if len(said) != 1 {
		t.Fatalf("a broken transport produced %d warnings: %v", len(said), said)
	}
	for _, wanted := range []string{"stopped early", "connection reset by peer", "no actions"} {
		if !strings.Contains(said[0], wanted) {
			t.Fatalf("the warning does not say %q: %s", wanted, said[0])
		}
	}

	quiet := &collector{}
	runReadLoop(t, testShepherd(quiet.warn), "")
	if said := quiet.said(); len(said) != 0 {
		t.Fatalf("a clean end of stream warned: %v", said)
	}
}

// A shepherd that goes away mid-stream has to stop the reader. The
// alternative is a loop running handlers whose replies nobody can write.
// ErrClosed is final here, never retried: a retry could send one reply
// twice.
func TestAWriterFailureStopsTheReaderMidStream(t *testing.T) {
	warnings := &collector{}
	shepherd := testShepherd(warnings.warn)
	ran := make(chan struct{}, 8)
	shepherd.OnAction("gc", func(Action) string {
		ran <- struct{}{}
		return "collected"
	})
	go shepherd.out.drain(failWriter{})

	// Waiting for the close is what makes the count below a fact.
	shepherd.out.pushLossy(NewMetric("rps", 1))
	select {
	case <-shepherd.out.closed:
	case <-time.After(serveDeadline):
		t.Fatal("a failing writer never closed the outbox")
	}

	runReadLoop(t, shepherd, strings.Repeat("{\"kind\":\"action\",\"name\":\"gc\",\"id\":1}\n", 8))

	if got := len(ran); got != 1 {
		t.Fatalf("%d handlers ran after the writer failed, want 1", got)
	}
	said := warnings.said()
	if len(said) != 1 || !strings.Contains(said[0], "no longer taking replies") {
		t.Fatalf("a reader stopped by a failing writer said %v", said)
	}
}

// Handlers run on the reader goroutine, so one that blocks holds up the
// next message. Nothing in this package bounds a handler. The ordering
// has to be a fact rather than a hope.
func TestABlockingHandlerHoldsUpTheNextMessage(t *testing.T) {
	shepherd := testShepherd(func(string) {})
	entered, release := make(chan struct{}), make(chan struct{})
	shepherd.OnAction("slow", func(Action) string {
		close(entered)
		<-release
		return "slow done"
	})
	shepherd.OnAction("quick", func(Action) string { return "quick done" })

	done := make(chan struct{})
	go func() {
		defer close(done)
		shepherd.readLoop(bufio.NewReader(strings.NewReader(
			"{\"kind\":\"action\",\"name\":\"slow\",\"id\":1}\n" +
				"{\"kind\":\"action\",\"name\":\"quick\",\"id\":2}\n")))
	}()

	select {
	case <-entered:
	case <-time.After(serveDeadline):
		t.Fatal("the first handler never ran")
	}
	select {
	case reply := <-shepherd.out.messages:
		t.Fatalf("the second message was answered while the first blocked: %+v", reply)
	default:
	}

	close(release)
	select {
	case <-done:
	case <-time.After(serveDeadline):
		t.Fatal("readLoop never returned")
	}
	first, second := takeReply(t, shepherd), takeReply(t, shepherd)
	if *first.Body != "slow done" || *second.Body != "quick done" {
		t.Fatalf("the replies are %q then %q", *first.Body, *second.Body)
	}
}

// A handler calling runtime.Goexit ends the goroutine its reply runs
// on, and nothing else. The action is still answered and the reader
// goes on to the next message.
func TestAHandlerThatEndsItsGoroutineStillAnswersAndTheReaderGoesOn(t *testing.T) {
	shepherd := testShepherd(func(string) {})
	shepherd.OnAction("leave", func(Action) string {
		runtime.Goexit()
		return "never reached"
	})
	shepherd.OnAction("after", func(Action) string { return "after ok" })

	runReadLoop(t, shepherd,
		"{\"kind\":\"action\",\"name\":\"leave\",\"id\":1}\n"+
			"{\"kind\":\"action\",\"name\":\"after\",\"id\":2}\n")

	first, second := takeReply(t, shepherd), takeReply(t, shepherd)
	if *first.Body != "action handler ended its goroutine" {
		t.Fatalf("the first body is %q", *first.Body)
	}
	if first.ID == nil || *first.ID != 1 {
		t.Fatalf("the id was not echoed: %+v", first.ID)
	}
	if *second.Body != "after ok" {
		t.Fatalf("the second body is %q, so the reader did not survive", *second.Body)
	}
}

// A shutdown carries no reply, so the warning is the only sign that a
// handler left. Losing the reader here is worse: the app was already
// being asked to stop.
func TestAShutdownHandlerThatEndsItsGoroutineWarnsAndTheReaderGoesOn(t *testing.T) {
	warnings := &collector{}
	shepherd := testShepherd(warnings.warn)
	shepherd.OnShutdown(func() { runtime.Goexit() })
	shepherd.OnAction("after", func(Action) string { return "after ok" })

	runReadLoop(t, shepherd,
		"{\"kind\":\"shutdown\"}\n"+
			"{\"kind\":\"action\",\"name\":\"after\",\"id\":1}\n")

	if reply := takeReply(t, shepherd); *reply.Body != "after ok" {
		t.Fatalf("the body is %q, so the reader did not survive", *reply.Body)
	}
	said := warnings.said()
	if len(said) != 1 || !strings.Contains(said[0], "shutdown handler ended its goroutine") {
		t.Fatalf("a shutdown handler that left produced %v", said)
	}
}
