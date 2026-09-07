package channel

import (
	"bufio"
	"io"
	"os"
	"sync/atomic"
)

// channelTaken guards the inherited channel against a second take.
//
// Claimed only by a call that goes on to open something. A refusal
// leaves it alone, or one bad descriptor would refuse every later call.
var channelTaken atomic.Bool

// Conn is the shepherd channel with no goroutines: the caller owns the
// loop.
//
// Serve is the documented default, because it answers an action nobody
// registered. Reach for Conn when the app already runs an event loop of
// its own.
//
// Not safe for concurrent use. One goroutine drives Recv and Send.
type Conn struct {
	reader  *bufio.Reader
	writer  io.Writer
	handle  io.Closer
	version string
	closed  bool
}

// Open takes this process's channel and hands back the low layer.
//
// ErrNoChannel means the operator opened none, which is ordinary rather
// than a failure. ErrAlreadyTaken means this process took its channel
// already, Serve included: one descriptor has one owner. ErrUnusable
// means the environment names a channel that cannot be opened here.
func Open() (*Conn, error) {
	return openConn(os.LookupEnv)
}

// openConn is Open with its environment injected, so a test needs no
// process-wide variable.
func openConn(get lookup) (*Conn, error) {
	found, err := discover(get)
	if err != nil {
		return nil, err
	}
	if found.Kind == EndpointAbsent {
		return nil, ErrNoChannel
	}
	if channelTaken.Swap(true) {
		return nil, ErrAlreadyTaken
	}
	var opened *connection
	if found.Kind == EndpointDescriptor {
		opened, err = openDescriptor(found.FD)
	} else {
		opened, err = openPipe(found.Pipe)
	}
	if err != nil {
		// Nothing was taken. Keeping the claim would refuse a later
		// call that might work.
		channelTaken.Store(false)
		return nil, err
	}
	version, _ := get(VersionVar)
	return &Conn{
		reader:  bufio.NewReader(opened.reader),
		writer:  opened.writer,
		handle:  opened.handle,
		version: version,
	}, nil
}

// Recv reads one message from the shepherd.
//
// io.EOF is the shepherd closing its end. A line that will not parse is
// ErrMalformed. The next call resumes at the line after it.
func (c *Conn) Recv() (ShepherdMessage, error) {
	if c.closed {
		return ShepherdMessage{}, ErrClosed
	}
	return readMessage(c.reader)
}

// Send writes one message and its newline.
//
// Blocks until the transport takes it. Nothing is queued here, so a slow
// shepherd is the caller's to handle.
func (c *Conn) Send(message ChildMessage) error {
	if c.closed {
		return ErrClosed
	}
	return writeMessage(c.writer, message)
}

// Version is the SHEP_CHANNEL_VERSION stamp, empty when there was none.
//
// A stamp, not a negotiation. An app can notice a wire it has never
// seen. It cannot ask for a different one.
func (c *Conn) Version() string { return c.version }

// Close releases the transport. A second call does nothing.
//
// Recv and Send return ErrClosed afterwards. The claim on the channel
// stays: shep opens one per process and never a second.
func (c *Conn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.handle.Close()
}

// intoHalves takes the channel apart for the two goroutines that drive
// it. Serve is the only caller.
func (c *Conn) intoHalves() (*bufio.Reader, io.Writer, string) {
	return c.reader, c.writer, c.version
}
