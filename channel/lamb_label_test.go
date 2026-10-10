package channel

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLabellingALambQueuesTheLabelForTheShepherd(t *testing.T) {
	shepherd := testShepherd(func(string) {})

	if err := shepherd.LabelLamb(4312, "worker 1"); err != nil {
		t.Fatalf("LabelLamb returned %v", err)
	}

	want := ChildMessage{Kind: KindLambLabel, PID: ptr(uint32(4312)), Label: ptr("worker 1")}
	if got := takeReply(t, shepherd); !reflect.DeepEqual(got, want) {
		t.Fatalf("queued %+v, want %+v", got, want)
	}
}

// The limit counts characters, as the shepherd does. A byte count would
// refuse a label the shepherd takes.
func TestALabelOfSixtyFourCharactersIsAccepted(t *testing.T) {
	label := strings.Repeat("é", 64)
	shepherd := testShepherd(func(string) {})

	if err := shepherd.LabelLamb(1, label); err != nil {
		t.Fatalf("LabelLamb refused 64 characters: %v", err)
	}
	if got := takeReply(t, shepherd); *got.Label != label {
		t.Fatalf("queued %q, want %q", *got.Label, label)
	}
}

func TestALabelThatBreaksTheGrammarIsRefusedAndNothingIsSent(t *testing.T) {
	cases := map[string]string{
		"sixty-five characters": strings.Repeat("é", 65),
		"a newline":             "worker\n1",
		"a tab":                 "worker\t1",
		"an escape":             "\x1b[31mworker",
		"a delete":              "worker\x7f",
		"a C1 control":          "worker\u009b1",
		"a NUL":                 "\x00",
		"invalid UTF-8":         "bad\xfflabel",
	}
	for name, label := range cases {
		shepherd := testShepherd(func(string) {})

		err := shepherd.LabelLamb(4312, label)

		if !errors.Is(err, ErrInvalidLabel) {
			t.Fatalf("%s: LabelLamb returned %v, want ErrInvalidLabel", name, err)
		}
		if queued := len(shepherd.out.messages); queued != 0 {
			t.Fatalf("%s: %d messages were queued for a refused label", name, queued)
		}
	}
}

// An app run without a channel in development meets the same refusal as
// one run under shep. The mistake shows before it ships.
func TestAnInertHandleRefusesABadLabelToo(t *testing.T) {
	shepherd := inert("", func(string) {})

	if err := shepherd.LabelLamb(4312, "worker\n1"); !errors.Is(err, ErrInvalidLabel) {
		t.Fatalf("LabelLamb returned %v, want ErrInvalidLabel", err)
	}
	if err := shepherd.LabelLamb(4312, "worker 1"); err != nil {
		t.Fatalf("an inert LabelLamb of a good label returned %v", err)
	}
}

// A label is state the operator reads. Unlike a metric it is not dropped
// when the queue is full, so the call waits for the writer to make room.
func TestLabellingALambWaitsForQueueRoomInsteadOfDropping(t *testing.T) {
	shepherd := testShepherd(func(string) {})
	shepherd.out = newOutbox(1)
	shepherd.Metric("fills-the-queue", 1)

	done := make(chan error, 1)
	go func() { done <- shepherd.LabelLamb(4312, "worker 1") }()

	select {
	case err := <-done:
		t.Fatalf("LabelLamb returned %v with the queue full", err)
	case <-time.After(100 * time.Millisecond):
	}

	if first := takeReply(t, shepherd); first.Kind != KindMetric {
		t.Fatalf("the first queued message is %q, want the metric", first.Kind)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("LabelLamb returned %v once there was room", err)
		}
	case <-time.After(serveDeadline):
		t.Fatal("LabelLamb stayed parked after the queue made room")
	}
	if got := takeReply(t, shepherd); got.Kind != KindLambLabel {
		t.Fatalf("queued %q, want the label", got.Kind)
	}
	if dropped := shepherd.DroppedMetrics(); dropped != 0 {
		t.Fatalf("%d messages were dropped, want none", dropped)
	}
}

// Without this, an app whose shepherd went away parks forever on a label.
func TestLabellingALambReportsErrClosedOnceTheShepherdIsGone(t *testing.T) {
	shepherd := testShepherd(func(string) {})
	shepherd.out.close()

	if err := shepherd.LabelLamb(4312, "worker 1"); !errors.Is(err, ErrClosed) {
		t.Fatalf("LabelLamb returned %v, want ErrClosed", err)
	}
}

// Empty is a value the shepherd acts on. Dropping the key would make it
// a malformed frame instead of a clear.
func TestAnEmptyLabelIsSentAndClears(t *testing.T) {
	shepherd := testShepherd(func(string) {})

	if err := shepherd.LabelLamb(4312, ""); err != nil {
		t.Fatalf("LabelLamb returned %v", err)
	}

	encoded, err := encodeLine(takeReply(t, shepherd))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if want := `{"kind":"lamb-label","pid":4312,"label":""}`; string(encoded) != want {
		t.Fatalf("the line is %s, want %s", encoded, want)
	}
}
