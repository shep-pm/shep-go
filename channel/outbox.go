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
// A dropped metric costs nothing: the shepherd logs metrics at debug
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
		o.dropped.Add(1)
		return
	}
	select {
	case o.messages <- message:
	default:
		o.dropped.Add(1)
	}
}

// pushBlocking queues a message that must not be lost, waiting for room.
//
// Returns ErrClosed once the writer has stopped, rather than parking on
// a queue nothing drains.
func (o *outbox) pushBlocking(message ChildMessage) error {
	if o.isClosed() {
		return ErrClosed
	}
	select {
	case o.messages <- message:
		return nil
	case <-o.closed:
		return ErrClosed
	}
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
// closes. It then writes whatever is still queued and returns.
func (o *outbox) drain(writer io.Writer) {
	defer o.close()
	for {
		select {
		case message := <-o.messages:
			if err := writeMessage(writer, message); err != nil {
				return
			}
		case <-o.closed:
			for {
				select {
				case message := <-o.messages:
					if err := writeMessage(writer, message); err != nil {
						return
					}
				default:
					return
				}
			}
		}
	}
}
