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

func TestAFullOutboxDropsAMetricAndCountsIt(t *testing.T) {
	out := newOutbox(1)
	out.pushLossy(NewMetric("rps", 1))
	out.pushLossy(NewMetric("rps", 2))
	out.pushLossy(NewMetric("rps", 3))

	if got := out.droppedCount(); got != 2 {
		t.Fatalf("dropped %d, want 2", got)
	}
	if message := <-out.messages; *message.Value != 1 {
		t.Fatalf("the queued sample is %v, want the first one", *message.Value)
	}
}

func TestALossyPushNeverBlocks(t *testing.T) {
	out := newOutbox(0)
	done := make(chan struct{})
	go func() {
		out.pushLossy(NewMetric("rps", 1))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(outboxDeadline):
		t.Fatal("pushLossy parked on a full outbox")
	}
	if got := out.droppedCount(); got != 1 {
		t.Fatalf("dropped %d, want 1", got)
	}
}

func TestAMustDeliverPushWaitsForRoomAndThenProceeds(t *testing.T) {
	out := newOutbox(1)
	if err := out.pushBlocking(NewReady()); err != nil {
		t.Fatalf("the first message did not fit: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- out.pushBlocking(NewReady()) }()

	select {
	case err := <-done:
		t.Fatalf("pushBlocking returned %v while the outbox was full", err)
	case <-time.After(100 * time.Millisecond):
	}

	<-out.messages
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
	if err := out.pushBlocking(NewReady()); err != nil {
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
	out.pushLossy(NewMetric("rps", 1))
	if got := out.droppedCount(); got != 1 {
		t.Fatalf("dropped %d, want 1", got)
	}
}

func TestAMustDeliverPushOnAClosedOutboxRefuses(t *testing.T) {
	out := newOutbox(4)
	out.close()
	if err := out.pushBlocking(NewReady()); !errors.Is(err, ErrClosed) {
		t.Fatalf("pushBlocking returned %v, want ErrClosed", err)
	}
}

// A reply queued just before the shepherd leaves still has to reach the
// wire. close is not a discard.
func TestDrainWritesWhatIsAlreadyQueuedAfterClose(t *testing.T) {
	out := newOutbox(4)
	if err := out.pushBlocking(NewReady()); err != nil {
		t.Fatalf("queue readiness: %v", err)
	}
	if err := out.pushBlocking(NewMetric("rps", 1)); err != nil {
		t.Fatalf("queue the metric: %v", err)
	}
	out.close()

	var written strings.Builder
	done := make(chan struct{})
	go func() {
		out.drain(&written)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(outboxDeadline):
		t.Fatal("drain never returned on a closed outbox")
	}

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
	if err := out.pushBlocking(NewReady()); err != nil {
		t.Fatalf("queue readiness: %v", err)
	}

	done := make(chan struct{})
	go func() {
		out.drain(failingWriter{})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(outboxDeadline):
		t.Fatal("drain never returned after a failed write")
	}
	if !out.isClosed() {
		t.Fatal("drain returned without closing the outbox")
	}
	if err := out.pushBlocking(NewReady()); !errors.Is(err, ErrClosed) {
		t.Fatalf("pushBlocking returned %v after a dead transport, want ErrClosed", err)
	}
}
