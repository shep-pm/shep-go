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

// startChild spawns the test binary as a supervised app on a real
// socketpair. It hands back the shepherd's end.
func startChild(t *testing.T) *child {
	t.Helper()
	pair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
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
		t.Fatalf("start the child: %v", err)
	}
	// Registered here, not below. A failure in between would otherwise
	// leave the child running with nothing to stop it.
	t.Cleanup(func() { cmd.Process.Kill() })
	if err := theirEnd.Close(); err != nil {
		t.Fatalf("close the child's end in the parent: %v", err)
	}

	conn, err := net.FileConn(ourEnd)
	if err != nil {
		t.Fatalf("wrap the shepherd's end: %v", err)
	}
	if err := ourEnd.Close(); err != nil {
		t.Fatalf("close our duplicate: %v", err)
	}
	if err := conn.SetDeadline(time.Now().Add(childDeadline)); err != nil {
		t.Fatalf("set the deadline: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &child{reader: bufio.NewReader(conn), writer: conn, cmd: cmd}
}
