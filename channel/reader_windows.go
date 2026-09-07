//go:build windows

package channel

import (
	"fmt"
	"io"
	"os"
	"syscall"
	"time"
	"unsafe"
)

// pipePollInterval is how long the reader waits between peeks at an empty
// pipe. Invisible next to action_timeout, which is seconds.
const pipePollInterval = 20 * time.Millisecond

// errPipeNotConnected is ERROR_PIPE_NOT_CONNECTED, which package syscall
// does not name. With ERROR_BROKEN_PIPE it is this channel's end of
// stream.
const errPipeNotConnected = syscall.Errno(233)

var (
	kernel32          = syscall.NewLazyDLL("kernel32.dll")
	procPeekNamedPipe = kernel32.NewProc("PeekNamedPipe")
)

// pipeReader reads the channel's pipe without parking inside ReadFile.
//
// The shepherd hands this process one pipe instance and the writer holds
// the same object. A parked read would hold it against every write. The
// halves are still not independent. A peek can wait behind an
// in-progress WriteFile.
type pipeReader struct {
	pipe *os.File
	// handle is taken once, at construction. os.File.Fd detaches the
	// file from the runtime poller for this process. The writer loses
	// SetWriteDeadline with it.
	handle uintptr
}

// buffered reports how many bytes are waiting. open is false once the
// shepherd has closed its end and every sent byte is drained.
func (r *pipeReader) buffered() (waiting uint32, open bool, err error) {
	var available uint32
	// LazyProc.Call carries //go:uintptrescapes. A pointer converted to
	// uintptr in this argument list stays alive and unmoved. That is
	// what makes &available sound here.
	reported, _, callErr := procPeekNamedPipe.Call(
		r.handle,
		0,
		0,
		0,
		uintptr(unsafe.Pointer(&available)),
		0,
	)
	if reported == 0 {
		errno, isErrno := callErr.(syscall.Errno)
		if isErrno && (errno == syscall.ERROR_BROKEN_PIPE || errno == errPipeNotConnected) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("peek the shepherd channel: %w", callErr)
	}
	return available, true, nil
}

// Read fills p with whatever the pipe already holds, waiting by polling.
func (r *pipeReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		waiting, open, err := r.buffered()
		if err != nil {
			return 0, err
		}
		if !open {
			return 0, io.EOF
		}
		if waiting == 0 {
			time.Sleep(pipePollInterval)
			continue
		}
		// Never asks for more than the peek reported. A larger read
		// parks in the kernel again and revives the deadlock.
		want := len(p)
		if int(waiting) < want {
			want = int(waiting)
		}
		return r.pipe.Read(p[:want])
	}
}

// openPipe opens the named pipe the shepherd created for this process.
func openPipe(path string) (*connection, error) {
	pipe, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: %s=%s could not be opened: %v", ErrUnusable, PipeVar, path, err)
	}
	return &connection{
		reader: &pipeReader{pipe: pipe, handle: pipe.Fd()},
		writer: pipe,
		handle: pipe,
	}, nil
}

// openDescriptor refuses an inherited descriptor on a platform that does
// not inherit one.
func openDescriptor(fd int) (*connection, error) {
	return nil, fmt.Errorf(
		"%w: %s=%d names an inherited descriptor and Windows does not inherit one", ErrUnusable, FDVar, fd)
}
