package channel

import "errors"

// ErrClosed means the channel is closed and the message was not sent.
//
// From Ready when the shepherd has gone away, and from a Conn whose
// Close has run.
var ErrClosed = errors.New("shep channel closed")

// ErrMalformed is one line the reader could not use.
//
// Recoverable. The next read resumes at the next line. That is what the
// shepherd does with a bad frame too.
var ErrMalformed = errors.New("malformed shepherd-channel frame")

// ErrUnusable means the environment names a channel that cannot be
// opened here.
//
// A broken environment rather than an absent one, so it is loud. Set
// SHEP_CHANNEL_PIPE on unix and this is what comes back.
var ErrUnusable = errors.New("unusable shepherd channel")

// ErrNoChannel means the operator opened no channel for this process.
//
// The ordinary case rather than a failure. Serve answers it with a
// handle whose every method does nothing.
var ErrNoChannel = errors.New("no shepherd channel on this process")

// ErrAlreadyTaken means this process already took its channel.
//
// One descriptor, one owner. Serve takes it through the same door, so a
// call after Serve meets this too.
var ErrAlreadyTaken = errors.New("the shepherd channel has already been taken by this process")
