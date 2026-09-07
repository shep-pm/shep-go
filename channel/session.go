package channel

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// readMessage reads one newline-delimited message.
//
// io.EOF with no bytes is the shepherd closing its end. A bad line is
// ErrMalformed, and the next call resumes at the line after it.
func readMessage(reader *bufio.Reader) (ShepherdMessage, error) {
	line, err := reader.ReadBytes('\n')
	if len(line) == 0 {
		if err == nil {
			err = io.EOF
		}
		return ShepherdMessage{}, err
	}
	// A final line with no newline arrives here with err set. bufio
	// stores that error for the next call. Dropping it now costs
	// nothing, and the last frame is not lost.
	return decodeMessage(line)
}

// decodeMessage turns one line into a message the reader can act on.
//
// The kind check is not a formality. A flat struct unmarshals any JSON
// object at all. This is what makes an action's Name and ID safe to
// dereference.
func decodeMessage(line []byte) (ShepherdMessage, error) {
	trimmed := bytes.TrimRight(line, "\r\n")
	if len(bytes.TrimSpace(trimmed)) == 0 {
		return ShepherdMessage{}, fmt.Errorf("%w: empty line", ErrMalformed)
	}
	var message ShepherdMessage
	if err := json.Unmarshal(trimmed, &message); err != nil {
		return ShepherdMessage{}, fmt.Errorf("%w: %s", ErrMalformed, err)
	}
	switch message.Kind {
	case KindShutdown:
		return message, nil
	case KindAction:
		if message.Name == nil || message.ID == nil {
			return ShepherdMessage{}, fmt.Errorf("%w: an action needs both name and id", ErrMalformed)
		}
		return message, nil
	default:
		return ShepherdMessage{}, fmt.Errorf("%w: unknown kind %q", ErrMalformed, message.Kind)
	}
}

// encodeLine encodes one message, with no trailing newline.
//
// HTML escaping is off. json.Marshal would write \u003c for a < in
// a reply body, and serde writes it verbatim.
func encodeLine(message any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(message); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buffer.Bytes(), "\n"), nil
}

// writeMessage writes one message and its newline.
func writeMessage(writer io.Writer, message ChildMessage) error {
	line, err := encodeLine(message)
	if err != nil {
		return err
	}
	_, err = writer.Write(append(line, '\n'))
	return err
}
