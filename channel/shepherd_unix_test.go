//go:build !windows

package channel

import (
	"bufio"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestStartOpensARealDescriptorAndSendsReadiness(t *testing.T) {
	appFD, shepherd := fakeShepherd(t)
	warnings := &collector{}

	handle := start(fakeEnv(map[string]string{
		FDVar:      fmt.Sprint(appFD),
		VersionVar: Version,
		nameVar:    "web",
	}), warnings.warn)

	if !handle.Active() {
		t.Fatal("a handle over a real descriptor is not active")
	}
	if handle.Version() != Version {
		t.Fatalf("Version is %q, want %q", handle.Version(), Version)
	}
	if err := handle.Ready(); err != nil {
		t.Fatalf("Ready: %v", err)
	}

	line, err := bufio.NewReader(shepherd).ReadString('\n')
	if err != nil {
		t.Fatalf("the shepherd never received readiness: %v", err)
	}
	if strings.TrimRight(line, "\r\n") != `{"kind":"ready"}` {
		t.Fatalf("the shepherd received %q", line)
	}
	if said := warnings.said(); len(said) != 0 {
		t.Fatalf("a working channel warned: %v", said)
	}
}

// D6: refusing would break every app on the day shep ships an additive 2.
func TestAnUnrecognisedVersionStampWarnsAndProceeds(t *testing.T) {
	appFD, shepherd := fakeShepherd(t)
	warnings := &collector{}

	handle := start(fakeEnv(map[string]string{
		FDVar:      fmt.Sprint(appFD),
		VersionVar: "99",
	}), warnings.warn)

	if err := handle.Ready(); err != nil {
		t.Fatalf("Ready after an unknown stamp: %v", err)
	}
	if _, err := bufio.NewReader(shepherd).ReadString('\n'); err != nil {
		t.Fatalf("the shepherd never received readiness: %v", err)
	}
	said := warnings.said()
	if len(said) != 1 || !strings.Contains(said[0], "99") {
		t.Fatalf("an unknown stamp produced %v", said)
	}
}

// The shepherd going away has to reach the app as ErrClosed, not as a
// parked call. Ready never retries on it: a retry could report readiness
// twice for one start.
func TestTheShepherdGoingAwayReachesTheAppAsErrClosed(t *testing.T) {
	appFD, shepherd := fakeShepherd(t)
	handle := start(fakeEnv(map[string]string{FDVar: fmt.Sprint(appFD)}), func(string) {})

	if err := shepherd.Close(); err != nil {
		t.Fatalf("close the shepherd's end: %v", err)
	}
	// Waiting for the reader to notice is what makes this a fact.
	select {
	case <-handle.out.closed:
	case <-time.After(serveDeadline):
		t.Fatal("the reader never noticed the shepherd go away")
	}

	if handle.Active() {
		t.Fatal("a handle whose shepherd went away still reads as live")
	}
	if err := handle.Ready(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Ready returned %v, want ErrClosed", err)
	}
}
