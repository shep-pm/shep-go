//go:build !windows

package channel

import (
	"fmt"
	"net"
	"os"
)

// openDescriptor takes the inherited socket and returns the open channel.
//
// Reads go straight to the conn. A socketpair's two ends are independent
// open file descriptions. A parked read costs a concurrent write
// nothing. net.FileConn hands the descriptor to the runtime poller.
func openDescriptor(fd int) (*connection, error) {
	file := os.NewFile(uintptr(fd), "shep-channel")
	if file == nil {
		return nil, fmt.Errorf("%w: %s=%d is not an open descriptor", ErrUnusable, FDVar, fd)
	}
	conn, err := net.FileConn(file)
	// FileConn duplicates the descriptor, so this closes only our copy.
	// Leaving it open would leak the number for the process's lifetime.
	closeErr := file.Close()
	if err != nil {
		return nil, fmt.Errorf(
			"%w: %s=%d is not an open socket (%v): the shepherd passes the channel as one end of a socketpair",
			ErrUnusable, FDVar, fd, err)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("%w: %s=%d could not be handed over: %v", ErrUnusable, FDVar, fd, closeErr)
	}
	return &connection{reader: conn, writer: conn, handle: conn}, nil
}

// openPipe refuses a Windows named pipe on a platform that has none.
func openPipe(path string) (*connection, error) {
	return nil, fmt.Errorf(
		"%w: %s=%s names a Windows named pipe and this is not Windows", ErrUnusable, PipeVar, path)
}
