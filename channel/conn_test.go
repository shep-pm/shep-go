package channel

import (
	"errors"
	"testing"
)

func TestOpenReportsNoChannelWhenNeitherVariableIsSet(t *testing.T) {
	conn, err := openConn(fakeEnv(map[string]string{nameVar: "web"}))
	if !errors.Is(err, ErrNoChannel) {
		t.Fatalf("openConn returned %v, want ErrNoChannel", err)
	}
	if conn != nil {
		t.Fatalf("openConn refused and handed back %+v", conn)
	}
	if channelTaken.Load() {
		t.Fatal("an absent channel was claimed")
	}
}

// The exported door reads the process environment. A descriptor discover
// refuses opens nothing, so this claims nothing either.
func TestOpenReadsTheProcessEnvironment(t *testing.T) {
	t.Setenv(FDVar, "1")
	if _, err := Open(); !errors.Is(err, ErrUnusable) {
		t.Fatalf("Open returned %v, want ErrUnusable", err)
	}
	if channelTaken.Load() {
		t.Fatal("a refused descriptor claimed the channel")
	}
}
