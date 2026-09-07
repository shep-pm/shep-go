package channel

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func readerOver(text string) *bufio.Reader {
	return bufio.NewReader(strings.NewReader(text))
}

func TestReadsTwoMessagesFromOneStream(t *testing.T) {
	reader := readerOver("{\"kind\":\"shutdown\"}\n{\"kind\":\"action\",\"name\":\"gc\",\"id\":7}\n")

	first, err := readMessage(reader)
	if err != nil {
		t.Fatalf("first message: %v", err)
	}
	if first.Kind != KindShutdown {
		t.Fatalf("first message is %q, want %q", first.Kind, KindShutdown)
	}

	second, err := readMessage(reader)
	if err != nil {
		t.Fatalf("second message: %v", err)
	}
	if second.Kind != KindAction || *second.Name != "gc" || *second.ID != 7 {
		t.Fatalf("second message is %+v", second)
	}
	if second.Params != nil {
		t.Fatalf("an action with no params decoded Params as %q", *second.Params)
	}

	if _, err := readMessage(reader); !errors.Is(err, io.EOF) {
		t.Fatalf("end of stream reported %v, want io.EOF", err)
	}
}

// The Windows transport is a byte-mode pipe, so a peer there may write
// \r\n.
func TestACarriageReturnBeforeTheNewlineIsTolerated(t *testing.T) {
	message, err := readMessage(readerOver("{\"kind\":\"shutdown\"}\r\n"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if message.Kind != KindShutdown {
		t.Fatalf("kind is %q, want %q", message.Kind, KindShutdown)
	}
}

// The shepherd skips a bad frame and keeps reading. This side has to
// match, or the two halves disagree about what one bad line costs.
func TestAMalformedLineIsRecoverable(t *testing.T) {
	reader := readerOver("not json\n{\"kind\":\"shutdown\"}\n")

	if _, err := readMessage(reader); !errors.Is(err, ErrMalformed) {
		t.Fatalf("a bad line reported %v, want ErrMalformed", err)
	}
	message, err := readMessage(reader)
	if err != nil {
		t.Fatalf("the reader did not resume after a bad line: %v", err)
	}
	if message.Kind != KindShutdown {
		t.Fatalf("kind is %q, want %q", message.Kind, KindShutdown)
	}
}

// A flat struct decodes any object at all. The kind check is what stands
// between an unknown message and a nil dereference.
func TestAnUnknownKindIsMalformed(t *testing.T) {
	_, err := readMessage(readerOver("{\"kind\":\"stampede\"}\n"))
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("an unknown kind reported %v, want ErrMalformed", err)
	}
	if !strings.Contains(err.Error(), "stampede") {
		t.Fatalf("the refusal does not name the kind: %v", err)
	}
}

func TestAnActionMissingItsNameOrIDIsMalformed(t *testing.T) {
	for _, line := range []string{
		"{\"kind\":\"action\",\"id\":7}\n",
		"{\"kind\":\"action\",\"name\":\"gc\"}\n",
	} {
		if _, err := readMessage(readerOver(line)); !errors.Is(err, ErrMalformed) {
			t.Fatalf("%s reported %v, want ErrMalformed", strings.TrimSpace(line), err)
		}
	}
}

func TestWritesOneLinePerMessageWithATrailingNewline(t *testing.T) {
	var out bytes.Buffer
	if err := writeMessage(&out, NewReady()); err != nil {
		t.Fatalf("write ready: %v", err)
	}
	if err := writeMessage(&out, NewMetric("rps", 42)); err != nil {
		t.Fatalf("write metric: %v", err)
	}
	want := "{\"kind\":\"ready\"}\n{\"kind\":\"metric\",\"name\":\"rps\",\"value\":42}\n"
	if out.String() != want {
		t.Fatalf("wrote %q, want %q", out.String(), want)
	}
}

// json.Marshal escapes <, > and & by default and serde does not. A reply
// body is free-form app text, so the two would drift on ordinary input.
func TestAReplyBodyKeepsItsAngleBracketsVerbatim(t *testing.T) {
	var out bytes.Buffer
	if err := writeMessage(&out, NewReply("gc", "freed <b>4</b> pages", ptr(uint64(7)))); err != nil {
		t.Fatalf("write reply: %v", err)
	}
	if !strings.Contains(out.String(), "freed <b>4</b> pages") {
		t.Fatalf("the body was escaped: %s", out.String())
	}
}

// A zero value must survive encoding. Go's omitempty drops a plain
// zero, so NewMetric wraps every value. This confirms the wrap
// actually reaches the wire.
func TestAZeroMetricValueSurvivesEncoding(t *testing.T) {
	var out bytes.Buffer
	if err := writeMessage(&out, NewMetric("errors", 0)); err != nil {
		t.Fatalf("write metric: %v", err)
	}
	if !strings.Contains(out.String(), `"value":0`) {
		t.Fatalf("a zero value did not survive encoding: %s", out.String())
	}
}
