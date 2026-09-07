//go:build !windows

package channel

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// connDeadline bounds every wait on a real socketpair below.
const connDeadline = 5 * time.Second

// releaseChannel drops this process's claim on the channel.
//
// The claim is process-global and Go runs a package's tests in one
// process. Production never releases it: shep opens one channel per
// process and never a second.
func releaseChannel() { channelTaken.Store(false) }

// fakeShepherd hands back a real socketpair. The library gets one end's
// descriptor number, and the test drives the other.
func fakeShepherd(t *testing.T) (appFD int, shepherd net.Conn) {
	t.Helper()
	pair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	ours := os.NewFile(uintptr(pair[0]), "shepherd-end")
	conn, err := net.FileConn(ours)
	if err != nil {
		t.Fatalf("wrap the shepherd's end: %v", err)
	}
	if err := ours.Close(); err != nil {
		t.Fatalf("close our duplicate: %v", err)
	}
	if err := conn.SetDeadline(time.Now().Add(connDeadline)); err != nil {
		t.Fatalf("set the deadline: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
		releaseChannel()
	})
	return pair[1], conn
}

// D1's reason for existing. An app that owns its event loop drives the
// channel from inside it. Nothing of ours runs beside it.
func TestAnAppDrivesRecvAndSendWithNoGoroutines(t *testing.T) {
	appFD, shepherd := fakeShepherd(t)
	fromApp := bufio.NewReader(shepherd)
	// A collection first. The runtime's own workers then exist before
	// the baseline, and cannot be counted as ours.
	runtime.GC()
	before := runtime.NumGoroutine()

	conn, err := openConn(fakeEnv(map[string]string{
		FDVar:      fmt.Sprint(appFD),
		VersionVar: Version,
	}))
	if err != nil {
		t.Fatalf("openConn: %v", err)
	}
	defer conn.Close()
	if conn.Version() != Version {
		t.Fatalf("Version is %q, want %q", conn.Version(), Version)
	}

	// Closing the shepherd's end ends a parked Recv. A regression fails
	// this test rather than hanging the suite.
	stop := time.AfterFunc(connDeadline, func() { shepherd.Close() })

	if err := conn.Send(NewReady()); err != nil {
		t.Fatalf("Send readiness: %v", err)
	}
	line, err := fromApp.ReadString('\n')
	if err != nil {
		t.Fatalf("the shepherd never received readiness: %v", err)
	}
	if strings.TrimRight(line, "\r\n") != `{"kind":"ready"}` {
		t.Fatalf("the shepherd received %q", line)
	}

	if _, err := shepherd.Write([]byte("{\"kind\":\"action\",\"name\":\"gc\",\"id\":7}\n")); err != nil {
		t.Fatalf("send the action: %v", err)
	}
	message, err := conn.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if message.Kind != KindAction || *message.Name != "gc" || *message.ID != 7 {
		t.Fatalf("Recv returned %+v", message)
	}
	if err := conn.Send(NewReply(*message.Name, "collected", message.ID)); err != nil {
		t.Fatalf("Send the reply: %v", err)
	}
	reply, err := fromApp.ReadString('\n')
	if err != nil {
		t.Fatalf("the shepherd never received the reply: %v", err)
	}
	if !strings.Contains(reply, `"id":7`) {
		t.Fatalf("the reply is %q", reply)
	}

	stop.Stop()
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("the low layer left %d goroutine(s) running", after-before)
	}
}

// D7's guard, at the door Serve opens through as well.
func TestASecondOpenIsRefused(t *testing.T) {
	appFD, _ := fakeShepherd(t)
	env := fakeEnv(map[string]string{FDVar: fmt.Sprint(appFD)})

	conn, err := openConn(env)
	if err != nil {
		t.Fatalf("the first open: %v", err)
	}
	defer conn.Close()

	if _, err := openConn(env); !errors.Is(err, ErrAlreadyTaken) {
		t.Fatalf("the second open returned %v, want ErrAlreadyTaken", err)
	}
}

func TestRecvAndSendRefuseAfterClose(t *testing.T) {
	appFD, _ := fakeShepherd(t)
	conn, err := openConn(fakeEnv(map[string]string{FDVar: fmt.Sprint(appFD)}))
	if err != nil {
		t.Fatalf("openConn: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("a second Close returned %v, want nil", err)
	}
	if _, err := conn.Recv(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Recv after Close returned %v, want ErrClosed", err)
	}
	if err := conn.Send(NewReady()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Send after Close returned %v, want ErrClosed", err)
	}
}

// An open that failed claimed nothing. Keeping the claim would refuse
// every later call in this process over one bad value.
func TestAFailedOpenReleasesTheClaim(t *testing.T) {
	// openDescriptor closes the number it was handed, so this file needs
	// no Close of its own.
	file, err := os.CreateTemp(t.TempDir(), "not-a-socket")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}

	_, err = openConn(fakeEnv(map[string]string{FDVar: fmt.Sprint(file.Fd())}))
	if !errors.Is(err, ErrUnusable) {
		t.Fatalf("a regular file opened as %v, want ErrUnusable", err)
	}
	if channelTaken.Load() {
		t.Fatal("a failed open kept the claim")
	}
}
