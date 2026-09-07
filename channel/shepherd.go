package channel

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// noChannelAdvice is what to tell an author running under shep with no
// channel.
const noChannelAdvice = "no channel on this process. Set `channel = true` " +
	"(or `wait_ready` / `shutdown_with_message`) on this app in the Flockfile to open one."

// unhandledShutdownAdvice is what to tell an author whose app was asked
// to stop and registered nothing.
const unhandledShutdownAdvice = "the shepherd sent shutdown and no OnShutdown handler is registered. " +
	"This process will be killed when kill_timeout expires. Register one to stop gracefully."

// Shepherd is a handle on this process's shepherd channel.
//
// Safe to use from any goroutine, and never nil. With no channel every
// method does nothing. An app needs no branch at its call sites.
type Shepherd struct {
	out      *outbox
	handlers *dispatch
	version  string
	warn     func(string)
}

var (
	serveOnce  sync.Once
	served     *Shepherd
	serveCalls atomic.Uint32
)

// Serve opens this process's channel and starts serving it.
//
// Always returns a usable handle. A second call returns the first one.
// The channel is one descriptor and cannot be owned twice.
func Serve() *Shepherd {
	return serveShared(os.LookupEnv, stderrWarn)
}

// serveShared is Serve with its environment and its warning sink
// injected. A test can call it twice and read what was said.
func serveShared(get lookup, warn func(string)) *Shepherd {
	serveOnce.Do(func() { served = start(get, warn) })
	if serveCalls.Add(1) == 2 {
		warn("Serve() called more than once; returning the first handle. " +
			"The channel is one descriptor and cannot be opened twice.")
	}
	return served
}

// stderrWarn writes one line where shep already collects it as bleats.
//
// The prefix is the same in every shep client library. An operator greps
// for one string whatever the app is written in.
func stderrWarn(message string) {
	fmt.Fprintln(os.Stderr, "shep-channel: "+message)
}

// inert is the handle an app gets with no channel.
func inert(version string, warn func(string)) *Shepherd {
	return &Shepherd{handlers: newDispatch(), version: version, warn: warn}
}

// start opens the channel through the low layer and spawns the two
// goroutines that drive it.
func start(get lookup, warn func(string)) *Shepherd {
	conn, err := openConn(get)
	if err != nil {
		if errors.Is(err, ErrNoChannel) {
			if _, underShep := get(nameVar); underShep {
				warn(noChannelAdvice)
			}
			return inert("", warn)
		}
		stamp, _ := get(VersionVar)
		warn(err.Error() + "; continuing without a channel")
		return inert(stamp, warn)
	}

	reader, writer, version := conn.intoHalves()
	if version != "" && version != Version {
		warn(fmt.Sprintf(
			"the shepherd stamps %s=%s and this module implements %s; continuing, since a newer wire has so far only added fields an older reader ignores",
			VersionVar, version, Version))
	}

	shepherd := &Shepherd{
		out:      newOutbox(outboxCapacity),
		handlers: newDispatch(),
		version:  version,
		warn:     warn,
	}
	go shepherd.out.drain(writer)
	go shepherd.readLoop(reader)
	return shepherd
}

// readLoop reads one message at a time and answers it.
//
// Handlers run on this goroutine, so a slow handler delays the next
// message. The shepherd's action_timeout is the budget for that.
func (s *Shepherd) readLoop(reader *bufio.Reader) {
	defer s.out.close()
	warnedMalformed := false
	for {
		message, err := readMessage(reader)
		if err != nil {
			if !errors.Is(err, ErrMalformed) {
				return
			}
			if !warnedMalformed {
				warnedMalformed = true
				s.warn(err.Error())
			}
			continue
		}
		if !s.answer(message) {
			return
		}
	}
}

// answer handles one message and reports whether the loop continues.
func (s *Shepherd) answer(message ShepherdMessage) bool {
	switch message.Kind {
	case KindShutdown:
		handler, registered := s.handlers.resolveShutdown()
		if !registered {
			s.warn(unhandledShutdownAdvice)
			return true
		}
		if panicked, failed := runShutdown(handler); failed {
			s.warn("shutdown handler panicked: " + panicked)
		}
		return true
	case KindAction:
		// decodeMessage refuses an action without both, so neither
		// dereference here can be nil.
		action := Action{Name: *message.Name, Params: message.Params}
		handler, registered := s.handlers.resolveAction(action.Name)
		body := replyBody(handler, registered, action)
		return s.out.pushBlocking(NewReply(action.Name, body, message.ID)) == nil
	default:
		// Unreachable: decodeMessage refuses every other kind.
		return true
	}
}

// Ready says this app is up. Blocks only until the message is queued.
//
// Returns ErrClosed when the shepherd has gone away. With no channel it
// returns nil: nothing to report, and no failure to handle.
func (s *Shepherd) Ready() error {
	if s.out == nil {
		return nil
	}
	return s.out.pushBlocking(NewReady())
}

// Metric records one sample. Never blocks and never fails.
//
// A sample may be dropped if the shepherd stops reading, which is what
// DroppedMetrics counts. That trade keeps a hot path off a full socket.
func (s *Shepherd) Metric(name string, value float64) {
	if s.out == nil {
		return
	}
	s.out.pushLossy(NewMetric(name, value))
}

// OnAction registers a handler for one action name, replacing any prior
// one. The returned string becomes the reply body.
//
// Safe to call from another goroutine, or from inside a handler. A
// reload action can swap its own handlers this way.
func (s *Shepherd) OnAction(name string, fn func(a Action) string) *Shepherd {
	s.handlers.registerAction(name, fn)
	return s
}

// OnShutdown registers the handler run when the shepherd asks this app
// to stop.
//
// Without one, a shutdown warns and nothing else happens. This package
// never ends a process on its own judgement.
func (s *Shepherd) OnShutdown(fn func()) *Shepherd {
	s.handlers.registerShutdown(fn)
	return s
}

// Active reports whether this process's channel is live right now.
//
// False before an operator opens one, and false again once the shepherd
// goes away. An app watching it notices, rather than reporting into
// nothing.
func (s *Shepherd) Active() bool {
	return s.out != nil && !s.out.isClosed()
}

// DroppedMetrics is how many samples were dropped because the shepherd
// was not keeping up. Always 0 without a channel.
func (s *Shepherd) DroppedMetrics() uint64 {
	if s.out == nil {
		return 0
	}
	return s.out.droppedCount()
}

// Version is the SHEP_CHANNEL_VERSION stamp, empty when the shepherd set
// none.
func (s *Shepherd) Version() string { return s.version }
