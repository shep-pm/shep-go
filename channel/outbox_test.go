package channel

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// outboxDeadline bounds every wait below. A working outbox answers in
// microseconds; this is slack for a loaded runner, not an expectation.
const outboxDeadline = 5 * time.Second

// failingWriter fails every write, so drain's give-up path needs no
// real transport.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("the shepherd went away") }

// waitFor fails at the deadline instead of parking on a signal.
func waitFor(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(outboxDeadline):
		t.Fatalf("%s never finished", what)
	}
}

// runBounded runs work on its own goroutine and fails at the deadline.
//
// Every call that could park goes through it, so a regression is a red
// test rather than a hung binary an external timeout has to kill.
func runBounded(t *testing.T, what string, work func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		work()
	}()
	waitFor(t, done, what)
}

// pushBounded runs one blocking push and fails at the deadline.
func pushBounded(t *testing.T, out *outbox, message ChildMessage, what string) error {
	t.Helper()
	var err error
	runBounded(t, what, func() { err = out.pushBlocking(message) })
	return err
}

// receiveBounded takes one queued message and fails at the deadline.
func receiveBounded(t *testing.T, out *outbox, what string) ChildMessage {
	t.Helper()
	select {
	case message := <-out.messages:
		return message
	case <-time.After(outboxDeadline):
		t.Fatalf("%s never arrived", what)
		return ChildMessage{}
	}
}

func TestAFullOutboxDropsAMetricAndCountsIt(t *testing.T) {
	out := newOutbox(1)
	runBounded(t, "three lossy pushes against a capacity of one", func() {
		out.pushLossy(NewMetric("rps", 1))
		out.pushLossy(NewMetric("rps", 2))
		out.pushLossy(NewMetric("rps", 3))
	})

	if got := out.droppedCount(); got != 2 {
		t.Fatalf("dropped %d, want 2", got)
	}
	if message := receiveBounded(t, out, "the queued sample"); *message.Value != 1 {
		t.Fatalf("the queued sample is %v, want the first one", *message.Value)
	}
}

func TestALossyPushNeverBlocks(t *testing.T) {
	out := newOutbox(0)
	runBounded(t, "pushLossy on a full outbox", func() {
		out.pushLossy(NewMetric("rps", 1))
	})
	if got := out.droppedCount(); got != 1 {
		t.Fatalf("dropped %d, want 1", got)
	}
}

func TestAMustDeliverPushWaitsForRoomAndThenProceeds(t *testing.T) {
	out := newOutbox(1)
	if err := pushBounded(t, out, NewReady(), "the first push"); err != nil {
		t.Fatalf("the first message did not fit: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- out.pushBlocking(NewReady()) }()

	select {
	case err := <-done:
		t.Fatalf("pushBlocking returned %v while the outbox was full", err)
	case <-time.After(100 * time.Millisecond):
	}

	receiveBounded(t, out, "the queued message")
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("pushBlocking after room: %v", err)
		}
	case <-time.After(outboxDeadline):
		t.Fatal("pushBlocking never proceeded after the outbox drained")
	}
}

// Without this, an app whose shepherd went away parks forever in Ready.
func TestClosingReleasesABlockedPushWithErrClosed(t *testing.T) {
	out := newOutbox(1)
	if err := pushBounded(t, out, NewReady(), "the first push"); err != nil {
		t.Fatalf("the first message did not fit: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- out.pushBlocking(NewReady()) }()

	select {
	case err := <-done:
		t.Fatalf("pushBlocking returned %v too early", err)
	case <-time.After(100 * time.Millisecond):
	}

	out.close()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("pushBlocking returned %v, want ErrClosed", err)
		}
	case <-time.After(outboxDeadline):
		t.Fatal("pushBlocking stayed parked after close")
	}
}

// Emitting a metric after the shepherd leaves is ordinary, not an error.
// The sample is gone, and droppedCount is how an app sees that.
func TestALossyPushAfterCloseCountsTheDrop(t *testing.T) {
	out := newOutbox(4)
	out.close()
	runBounded(t, "pushLossy on a closed outbox", func() {
		out.pushLossy(NewMetric("rps", 1))
	})
	if got := out.droppedCount(); got != 1 {
		t.Fatalf("dropped %d, want 1", got)
	}
}

func TestAMustDeliverPushOnAClosedOutboxRefuses(t *testing.T) {
	out := newOutbox(4)
	out.close()
	err := pushBounded(t, out, NewReady(), "a push on a closed outbox")
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("pushBlocking returned %v, want ErrClosed", err)
	}
}

// A reply queued just before the shepherd leaves still has to reach the
// wire. close is not a discard.
func TestDrainWritesWhatIsAlreadyQueuedAfterClose(t *testing.T) {
	out := newOutbox(4)
	if err := pushBounded(t, out, NewReady(), "queueing readiness"); err != nil {
		t.Fatalf("queue readiness: %v", err)
	}
	if err := pushBounded(t, out, NewMetric("rps", 1), "queueing the metric"); err != nil {
		t.Fatalf("queue the metric: %v", err)
	}
	out.close()

	var written strings.Builder
	runBounded(t, "drain on a closed outbox", func() { out.drain(&written) })

	lines := strings.Split(strings.TrimSuffix(written.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("drain wrote %d lines, want 2: %q", len(lines), written.String())
	}
	if !strings.Contains(lines[0], `"kind":"ready"`) || !strings.Contains(lines[1], `"kind":"metric"`) {
		t.Fatalf("drain wrote %q", written.String())
	}
}

// A failed write means the transport is gone. Closing there is what stops
// Ready reporting success for a message nothing will ever send.
func TestDrainClosesTheOutboxWhenAWriteFails(t *testing.T) {
	out := newOutbox(4)
	if err := pushBounded(t, out, NewReady(), "queueing readiness"); err != nil {
		t.Fatalf("queue readiness: %v", err)
	}

	runBounded(t, "drain against a dead transport", func() { out.drain(failingWriter{}) })
	if !out.isClosed() {
		t.Fatal("drain returned without closing the outbox")
	}
	err := pushBounded(t, out, NewReady(), "a push after a dead transport")
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("pushBlocking returned %v after a dead transport, want ErrClosed", err)
	}
}
