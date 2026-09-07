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
