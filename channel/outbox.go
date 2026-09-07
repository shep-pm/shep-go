package channel

import (
	"io"
	"sync"
	"sync/atomic"
)

// outboxCapacity is how many messages may wait for the writer.
//
// ChildMessage is a handful of pointers. A full queue costs tens of
// kilobytes, plus what the names and bodies hold.
const outboxCapacity = 1024

// outbox is the queue between the app's goroutines and the one goroutine
// that writes.
//
// A dropped metric costs nothing. The shepherd logs metrics at debug
// level and reads them nowhere else. A dropped readiness hangs
// wait_ready, and a dropped reply costs an operator a whole
// action_timeout.
type outbox struct {
	messages  chan ChildMessage
	closed    chan struct{}
	closeOnce sync.Once
	dropped   atomic.Uint64
}

func newOutbox(capacity int) *outbox {
	return &outbox{
		messages: make(chan ChildMessage, capacity),
		closed:   make(chan struct{}),
	}
}

// pushLossy queues a message that may be dropped. Never blocks.
func (o *outbox) pushLossy(message ChildMessage) {
	if o.isClosed() {
		o.countDrop()
		return
	}
	select {
	case o.messages <- message:
	default:
		o.countDrop()
	}
}

// countDrop records one message discarded instead of sent.
func (o *outbox) countDrop() { o.dropped.Add(1) }

// pushBlocking queues a message that must not be lost, waiting for room.
//
// Returns ErrClosed once the writer has stopped, rather than parking on
// a queue nothing drains. A send and a close can both be ready. A
// winning send is rechecked, so nil never covers a stranded message.
func (o *outbox) pushBlocking(message ChildMessage) error {
	if o.isClosed() {
		return ErrClosed
	}
	select {
	case o.messages <- message:
	case <-o.closed:
		return ErrClosed
	}
	if o.isClosed() {
		return ErrClosed
	}
	return nil
}

// close releases every waiter. Safe to call more than once.
//
// The message channel itself is never closed. A send racing a close on
// it would panic, and a metric during shutdown is ordinary.
func (o *outbox) close() {
	o.closeOnce.Do(func() { close(o.closed) })
}

func (o *outbox) isClosed() bool {
	select {
	case <-o.closed:
		return true
	default:
		return false
	}
}

// droppedCount is how many messages pushLossy has discarded.
func (o *outbox) droppedCount() uint64 { return o.dropped.Load() }

// drain writes queued messages until the transport fails or the outbox
// closes.
//
// A close is not a discard: whatever is still queued is written first.
// A failed transport drops the rest, since nothing can reach it.
func (o *outbox) drain(writer io.Writer) {
	for {
		select {
		case message := <-o.messages:
			if !o.writeOrClose(writer, message) {
				return
			}
		case <-o.closed:
			for {
				select {
				case message := <-o.messages:
					if !o.writeOrClose(writer, message) {
						return
					}
				default:
					return
				}
			}
		}
	}
}

// writeOrClose writes one message, reporting whether the transport is
// still usable.
//
// The two failures differ. A message that will not encode costs one
// drop and nothing else. A write failure is the transport itself. The
// outbox closes, and a push racing that is told ErrClosed.
func (o *outbox) writeOrClose(writer io.Writer, message ChildMessage) bool {
	line, err := encodeLine(message)
	if err != nil {
		o.countDrop()
		return true
	}
	if err := writeLine(writer, line); err != nil {
		o.close()
		return false
	}
	return true
}
