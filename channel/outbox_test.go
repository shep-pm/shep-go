package channel

import (
	"errors"
	"math"
	"strings"
	"sync"
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
// Every call that could park goes through it. A regression is then a
// red test, not a hung binary an external timeout kills.
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

// sentinelName marks the message that divides one queue. Pushes before
// a close land ahead of it, and pushes after it land behind.
const sentinelName = "sentinel"

// queuedAfterTheSentinel reports which senders landed behind the mark.
//
// Every sender has finished by the time this runs. Draining the queue
// to empty then reads all of it.
func queuedAfterTheSentinel(t *testing.T, out *outbox) []int {
	t.Helper()
	var late []int
	past := false
	for {
		select {
		case message := <-out.messages:
			if *message.Name == sentinelName {
				past = true
			} else if past {
				late = append(late, int(*message.Value))
			}
		default:
			return late
		}
	}
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

// D4: nothing that must not be lost is reported as sent once the outbox
// has closed.
//
// close can land between a push's check and its send. The send then
// wins a race, and must not be reported as nil.
func TestABlockingPushNeverReportsSuccessOnceTheOutboxIsClosed(t *testing.T) {
	const trials = 50
	const senders = 512

	for range trials {
		out := newOutbox(outboxCapacity)
		gate := make(chan struct{})
		results := make([]error, senders)
		var pushing sync.WaitGroup
		for sender := range senders {
			pushing.Add(1)
			go func() {
				defer pushing.Done()
				<-gate
				results[sender] = out.pushBlocking(NewMetric("push", float64(sender)))
			}()
		}

		// close lands mid-flight, and the sentinel marks the queue at
		// that instant. The queue is first in, first out, so anything
		// behind the sentinel was queued later.
		close(gate)
		out.close()
		out.messages <- NewMetric(sentinelName, 0)

		finished := make(chan struct{})
		go func() {
			pushing.Wait()
			close(finished)
		}()
		waitFor(t, finished, "the senders")

		for _, sender := range queuedAfterTheSentinel(t, out) {
			if results[sender] == nil {
				t.Fatalf("pushBlocking reported message %d sent, and it was queued after close", sender)
			}
		}
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

// A metric JSON cannot carry is one lost sample, not a dead transport.
// encoding/json refuses NaN, and the outbox must not read that as a
// dead socket. Everything queued behind it still has to be written.
func TestDrainCountsAMetricItCannotEncodeAndWritesTheRest(t *testing.T) {
	out := newOutbox(4)
	if err := pushBounded(t, out, NewMetric("p99", math.NaN()), "queueing a metric JSON cannot carry"); err != nil {
		t.Fatalf("queue the metric: %v", err)
	}
	if err := pushBounded(t, out, NewReady(), "queueing readiness"); err != nil {
		t.Fatalf("queue readiness: %v", err)
	}
	out.close()

	var written strings.Builder
	runBounded(t, "drain over a metric that will not encode", func() { out.drain(&written) })

	if got := out.droppedCount(); got != 1 {
		t.Fatalf("dropped %d, want 1", got)
	}
	if written.String() != "{\"kind\":\"ready\"}\n" {
		t.Fatalf("drain wrote %q, want readiness alone", written.String())
	}
}
