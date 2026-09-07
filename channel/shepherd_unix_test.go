//go:build !windows

package channel

import (
	"bufio"
	"fmt"
	"strings"
	"testing"
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
