//go:build !windows

package channel

import (
	"bufio"
	"io"
	"net"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// child is the shepherd's side of a supervised process.
type child struct {
	reader *bufio.Reader
	writer io.Writer
	cmd    *exec.Cmd
}

// socketpairCloseOnExec opens a connected socketpair with both ends
// marked close-on-exec.
//
// syscall.Socketpair does not set FD_CLOEXEC, so an unmarked end is
// inherited by any child a concurrent fork spawns. ForkLock closes that
// window: nothing else can fork between the call and the marking.
func socketpairCloseOnExec() ([2]int, error) {
	syscall.ForkLock.Lock()
	defer syscall.ForkLock.Unlock()
	pair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return pair, err
	}
	syscall.CloseOnExec(pair[0])
	syscall.CloseOnExec(pair[1])
	return pair, nil
}

// startChild spawns the test binary as a supervised app on a real
// socketpair. It hands back the shepherd's end.
func startChild(t *testing.T) *child {
	t.Helper()
	pair, err := socketpairCloseOnExec()
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	ourEnd := os.NewFile(uintptr(pair[0]), "shepherd-end")
	theirEnd := os.NewFile(uintptr(pair[1]), "app-end")

	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		childGuard+"=1",
		FDVar+"=3",
		VersionVar+"="+Version,
		nameVar+"=answers",
	)
	// ExtraFiles[0] becomes the child's fd 3, which is where the
	// shepherd puts the channel.
	cmd.ExtraFiles = []*os.File{theirEnd}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		ourEnd.Close()
		theirEnd.Close()
		t.Fatalf("start the child: %v", err)
	}
	// Registered here, not below. A failure in between would otherwise
	// leave the child running with nothing to stop it.
	t.Cleanup(func() { cmd.Process.Kill() })
	if err := theirEnd.Close(); err != nil {
		ourEnd.Close()
		t.Fatalf("close the child's end in the parent: %v", err)
	}

	conn, err := net.FileConn(ourEnd)
	if err != nil {
		ourEnd.Close()
		t.Fatalf("wrap the shepherd's end: %v", err)
	}
	if err := ourEnd.Close(); err != nil {
		conn.Close()
		t.Fatalf("close our duplicate: %v", err)
	}
	if err := conn.SetDeadline(time.Now().Add(childDeadline)); err != nil {
		conn.Close()
		t.Fatalf("set the deadline: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &child{reader: bufio.NewReader(conn), writer: conn, cmd: cmd}
}

// eofChildGuard selects runAsEOFChild instead of the full shepherd
// handshake.
const eofChildGuard = "2"

// runAsEOFChild reads fd 3 directly and exits once it sees end of
// stream.
//
// It bypasses the shepherd's dispatcher so the test proves the
// descriptor itself carries EOF, not the message loop's handling of it.
func runAsEOFChild() {
	channel := os.NewFile(3, "channel")
	buffer := make([]byte, 1)
	for {
		if _, err := channel.Read(buffer); err != nil {
			os.Exit(0)
		}
	}
}

// maybeRunAsEOFChild runs runAsEOFChild if guard selects it, reporting
// whether it did.
func maybeRunAsEOFChild(guard string) bool {
	if guard != eofChildGuard {
		return false
	}
	runAsEOFChild()
	return true
}

// Without a close-on-exec socketpair, the child inherits a stray copy
// of the shepherd's own end. That extra reference keeps the socket
// open, so the child's read on fd 3 never returns EOF.
func TestClosingTheShepherdsEndSignalsEOFToTheChild(t *testing.T) {
	pair, err := socketpairCloseOnExec()
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	ourEnd := os.NewFile(uintptr(pair[0]), "shepherd-end")
	theirEnd := os.NewFile(uintptr(pair[1]), "app-end")

	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), childGuard+"="+eofChildGuard)
	cmd.ExtraFiles = []*os.File{theirEnd}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		ourEnd.Close()
		theirEnd.Close()
		t.Fatalf("start the child: %v", err)
	}
	t.Cleanup(func() { cmd.Process.Kill() })
	if err := theirEnd.Close(); err != nil {
		ourEnd.Close()
		t.Fatalf("close the child's end in the parent: %v", err)
	}

	if err := ourEnd.Close(); err != nil {
		t.Fatalf("close the shepherd's end: %v", err)
	}

	waitForExit(t, cmd)
}
