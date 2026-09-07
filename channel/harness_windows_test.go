//go:build windows

package channel

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

var (
	procCreateNamedPipeW = kernel32.NewProc("CreateNamedPipeW")
	procConnectNamedPipe = kernel32.NewProc("ConnectNamedPipe")
)

const (
	pipeAccessDuplex = 0x00000003
	pipeTypeByte     = 0x00000000
	pipeWait         = 0x00000000
	pipeBufferBytes  = 4096
	// errorPipeConnected means the client connected before
	// ConnectNamedPipe was called, which is a success.
	errorPipeConnected = syscall.Errno(535)
)

// child is the shepherd's side of a supervised process.
type child struct {
	reader *bufio.Reader
	writer io.Writer
	cmd    *exec.Cmd
}

// startChild creates one pipe instance and spawns the test binary
// against it. It hands back the shepherd's end.
//
// One instance, like the shepherd. The reader and the writer then share
// a handle, which is what the peek survives.
func startChild(t *testing.T) *child {
	t.Helper()
	name := fmt.Sprintf(`\\.\pipe\shep-go-channel-%d-%d`, os.Getpid(), time.Now().UnixNano())
	wide, err := syscall.UTF16PtrFromString(name)
	if err != nil {
		t.Fatalf("encode the pipe name: %v", err)
	}
	handle, _, callErr := procCreateNamedPipeW.Call(
		uintptr(unsafe.Pointer(wide)),
		pipeAccessDuplex,
		pipeTypeByte|pipeWait,
		1,
		pipeBufferBytes,
		pipeBufferBytes,
		0,
		0,
	)
	if handle == uintptr(syscall.InvalidHandle) {
		t.Fatalf("CreateNamedPipeW: %v", callErr)
	}
	server := os.NewFile(handle, name)
	t.Cleanup(func() { server.Close() })

	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		childGuard+"=1",
		PipeVar+"="+name,
		VersionVar+"="+Version,
		nameVar+"=answers",
	)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start the child: %v", err)
	}
	t.Cleanup(func() { cmd.Process.Kill() })

	connected := make(chan error, 1)
	go func() {
		reported, _, connectErr := procConnectNamedPipe.Call(handle, 0)
		if reported == 0 && connectErr != errorPipeConnected {
			connected <- connectErr
			return
		}
		connected <- nil
	}()
	select {
	case err := <-connected:
		if err != nil {
			t.Fatalf("ConnectNamedPipe: %v", err)
		}
	case <-time.After(childDeadline):
		t.Fatalf("the child never opened the pipe within %s", childDeadline)
	}

	return &child{reader: bufio.NewReader(server), writer: server, cmd: cmd}
}
