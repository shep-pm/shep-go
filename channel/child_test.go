package channel

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"
)

// childGuard makes the test binary run as the supervised app instead of
// running the suite. os/exec's own tests re-exec themselves this way.
const childGuard = "SHEP_GO_CHANNEL_CHILD"

// childDeadline bounds every wait on the child. A working app answers in
// milliseconds; this is slack for a loaded runner.
const childDeadline = 20 * time.Second

func TestMain(m *testing.M) {
	guard := os.Getenv(childGuard)
	if guard == "1" {
		runAsChild()
		return
	}
	if maybeRunAsEOFChild(guard) {
		return
	}
	os.Exit(m.Run())
}

// runAsChild is the supervised app the test below drives.
func runAsChild() {
	shepherd := Serve()
	shepherd.OnAction("gc", func(a Action) string {
		return "collected, fields=" + strings.Join(a.Fields(), ",")
	})
	shepherd.OnShutdown(func() { os.Exit(0) })
	if err := shepherd.Ready(); err != nil {
		fmt.Fprintln(os.Stderr, "ready:", err)
		os.Exit(1)
	}
	shepherd.Metric("rps", 42)
	// Parks the main goroutine. The reader goroutine does the work. A
	// pending timer keeps the runtime from calling this a deadlock.
	for {
		time.Sleep(time.Hour)
	}
}

// readLineWithin reads one line, or fails at the deadline. A hung child
// has to fail the test rather than park the suite.
func readLineWithin(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	type outcome struct {
		line string
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		line, err := reader.ReadString('\n')
		done <- outcome{line: line, err: err}
	}()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("read from the child: %v", got.err)
		}
		return strings.TrimRight(got.line, "\r\n")
	case <-time.After(childDeadline):
		t.Fatalf("the child did not answer within %s", childDeadline)
		return ""
	}
}

// assertSameMessage compares two lines key by key. Hazard 1 rules out a
// byte comparison: Go writes 42 where the Rust fixtures write 42.0.
func assertSameMessage(t *testing.T, got, want string) {
	t.Helper()
	var gotFields, wantFields map[string]any
	if err := json.Unmarshal([]byte(got), &gotFields); err != nil {
		t.Fatalf("the child wrote %q, which is not a JSON object: %v", got, err)
	}
	if err := json.Unmarshal([]byte(want), &wantFields); err != nil {
		t.Fatalf("the expectation %q is not a JSON object: %v", want, err)
	}
	if !reflect.DeepEqual(gotFields, wantFields) {
		t.Fatalf("the child wrote %v, want %v", gotFields, wantFields)
	}
}

// waitForExit waits for the child to stop, or fails at the deadline.
func waitForExit(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the child exited badly: %v", err)
		}
	case <-time.After(childDeadline):
		cmd.Process.Kill()
		t.Fatalf("the child did not stop within %s of a shutdown", childDeadline)
	}
}

func TestARealChildAnswersOnTheChannel(t *testing.T) {
	child := startChild(t)

	assertSameMessage(t, readLineWithin(t, child.reader), `{"kind":"ready"}`)
	assertSameMessage(t, readLineWithin(t, child.reader), `{"kind":"metric","name":"rps","value":42.0}`)

	if _, err := child.writer.Write([]byte("{\"kind\":\"action\",\"name\":\"gc\",\"params\":\"now please\",\"id\":7}\n")); err != nil {
		t.Fatalf("send the action: %v", err)
	}
	assertSameMessage(t, readLineWithin(t, child.reader),
		`{"kind":"action-reply","action":"gc","body":"collected, fields=now,please","id":7}`)

	// The rule this module exists for, against a real process.
	if _, err := child.writer.Write([]byte("{\"kind\":\"action\",\"name\":\"typo\",\"id\":8}\n")); err != nil {
		t.Fatalf("send the unknown action: %v", err)
	}
	assertSameMessage(t, readLineWithin(t, child.reader),
		`{"kind":"action-reply","action":"typo","body":"unknown action: typo","id":8}`)

	if _, err := child.writer.Write([]byte("{\"kind\":\"shutdown\"}\n")); err != nil {
		t.Fatalf("send the shutdown: %v", err)
	}
	waitForExit(t, child.cmd)
}
