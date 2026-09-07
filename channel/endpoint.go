package channel

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

const (
	// FDVar names the inherited descriptor. Set on unix only.
	FDVar = "SHEP_CHANNEL_FD"
	// PipeVar names the pipe path. Set on Windows only.
	PipeVar = "SHEP_CHANNEL_PIPE"
	// VersionVar carries the wire stamp whenever a channel exists.
	VersionVar = "SHEP_CHANNEL_VERSION"
	// nameVar is injected by shep and cannot be set by hand. So it
	// answers whether this process runs under shep.
	nameVar = "SHEP_NAME"
)

// firstInheritableFD is the lowest number the channel can arrive on. The
// app's own standard streams are 0, 1 and 2.
const firstInheritableFD = 3

// EndpointKind says which door a channel is behind, if there is one.
type EndpointKind int

const (
	// EndpointAbsent means the operator opened no channel here.
	EndpointAbsent EndpointKind = iota
	// EndpointDescriptor means an inherited descriptor, from FDVar.
	EndpointDescriptor
	// EndpointPipe means a named pipe path, from PipeVar.
	EndpointPipe
)

// String names the kind. A refusal and a failed assertion then read as
// words, not as a number.
func (k EndpointKind) String() string {
	switch k {
	case EndpointAbsent:
		return "absent"
	case EndpointDescriptor:
		return "descriptor"
	case EndpointPipe:
		return "pipe"
	default:
		return fmt.Sprintf("EndpointKind(%d)", int(k))
	}
}

// Endpoint is where this process's channel is, if it has one.
type Endpoint struct {
	// Kind says which of the two fields below carries meaning.
	Kind EndpointKind
	// FD is the inherited descriptor, set when Kind is
	// EndpointDescriptor.
	FD int
	// Pipe is the named pipe path, set when Kind is EndpointPipe.
	Pipe string
}

// lookup reads one environment variable. os.LookupEnv in production, a
// map in tests, so no test mutates the process environment.
type lookup func(string) (string, bool)

// connection is an open channel. Every half is the same object on unix;
// on Windows the reader wraps it.
type connection struct {
	reader io.Reader
	writer io.Writer
	handle io.Closer
}

// Discover reads the environment and says where the channel is.
//
// Branches on which variable is present, never on the platform: the
// shepherd sets exactly one of them, and neither is the ordinary case.
func Discover() (Endpoint, error) {
	return discover(os.LookupEnv)
}

// discover is Discover with its environment injected.
func discover(get lookup) (Endpoint, error) {
	if raw, set := get(FDVar); set {
		fd, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			return Endpoint{}, fmt.Errorf("%w: %s=%s is not a descriptor", ErrUnusable, FDVar, raw)
		}
		if fd < firstInheritableFD {
			return Endpoint{}, fmt.Errorf(
				"%w: %s=%d must be %d or above, since 0, 1 and 2 are this process's own standard streams",
				ErrUnusable, FDVar, fd, firstInheritableFD)
		}
		return Endpoint{Kind: EndpointDescriptor, FD: fd}, nil
	}
	if raw, set := get(PipeVar); set {
		if strings.TrimSpace(raw) == "" {
			return Endpoint{}, fmt.Errorf("%w: %s is set and empty", ErrUnusable, PipeVar)
		}
		return Endpoint{Kind: EndpointPipe, Pipe: raw}, nil
	}
	return Endpoint{Kind: EndpointAbsent}, nil
}
