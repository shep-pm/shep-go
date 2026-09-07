package channel

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// childCases maps a fixture name to the value its bytes mean.
var childCases = map[string]ChildMessage{
	"child-ready":           {Kind: KindReady},
	"child-metric":          {Kind: KindMetric, Name: ptr("rps"), Value: ptr(42.0)},
	"child-metric-zero":     {Kind: KindMetric, Name: ptr("idle"), Value: ptr(0.0)},
	"child-action-reply":    {Kind: KindActionReply, Action: ptr("gc"), Body: ptr("ok")},
	"child-action-reply-id": {Kind: KindActionReply, Action: ptr("gc"), Body: ptr("ok"), ID: ptr(uint64(7))},
}

// shepherdCases maps a fixture name to the value its bytes mean.
var shepherdCases = map[string]ShepherdMessage{
	"shepherd-shutdown":      {Kind: KindShutdown},
	"shepherd-action":        {Kind: KindAction, Name: ptr("gc"), ID: ptr(uint64(7))},
	"shepherd-action-params": {Kind: KindAction, Name: ptr("set-log-level"), Params: ptr("debug"), ID: ptr(uint64(8))},
}

func fixtureBytes(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("fixtures", name+".json"))
	if err != nil {
		t.Fatalf("read the fixture: %v", err)
	}
	return raw
}

// asFields decodes one line into its keys and values. Two encodings of
// one message then compare equal however each side spells a number.
func asFields(t *testing.T, what string, raw []byte) map[string]any {
	t.Helper()
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("%s is not a JSON object: %v", what, err)
	}
	return fields
}

// Fails when the vendored corpus gains or loses a file. A new fixture
// upstream is a case this suite has to decide about. Ignoring it because
// no test names it is not an option.
func TestEveryVendoredFixtureIsCovered(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("fixtures", "*.json"))
	if err != nil {
		t.Fatalf("glob the corpus: %v", err)
	}
	var onDisk []string
	for _, path := range paths {
		onDisk = append(onDisk, strings.TrimSuffix(filepath.Base(path), ".json"))
	}
	var covered []string
	for name := range childCases {
		covered = append(covered, name)
	}
	for name := range shepherdCases {
		covered = append(covered, name)
	}
	sort.Strings(onDisk)
	sort.Strings(covered)
	if !reflect.DeepEqual(onDisk, covered) {
		t.Fatalf("the corpus holds %v and this suite covers %v", onDisk, covered)
	}
}

// The decode direction is exact: these bytes must produce exactly these
// values, pointers and all.
func TestFixturesDecodeToTheExpectedValues(t *testing.T) {
	for name, want := range childCases {
		var got ChildMessage
		if err := json.Unmarshal(fixtureBytes(t, name), &got); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s decoded to %+v, want %+v", name, got, want)
		}
	}
	for name, want := range shepherdCases {
		var got ShepherdMessage
		if err := json.Unmarshal(fixtureBytes(t, name), &got); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s decoded to %+v, want %+v", name, got, want)
		}
	}
}

// The encode direction is compared key by key rather than byte for byte.
// Go writes 42 where serde writes 42.0. Both are valid, and no care here
// would close that gap.
func TestFixturesEncodeToTheSameMessage(t *testing.T) {
	check := func(name string, value any) {
		t.Helper()
		encoded, err := encodeLine(value)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got := asFields(t, "the encoded "+name, encoded)
		want := asFields(t, "the committed "+name, fixtureBytes(t, name))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s encoded to %v, want %v", name, got, want)
		}
	}
	for name, value := range childCases {
		check(name, value)
	}
	for name, value := range shepherdCases {
		check(name, value)
	}
}

// Hazard 2, independent of the corpus. With a plain float64 field, a
// metric of 0 marshals with no value key. The sample is gone.
func TestAZeroMetricKeepsItsValueOnTheWire(t *testing.T) {
	encoded, err := encodeLine(NewMetric("idle", 0))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	fields := asFields(t, "the encoded metric", encoded)
	value, present := fields["value"]
	if !present {
		t.Fatalf("a metric of zero encoded without a value key: %s", encoded)
	}
	if value != float64(0) {
		t.Fatalf("value is %#v, want 0", value)
	}
}

// Ties the corpus to the constructors the library sends, not to literals
// only this file writes.
func TestTheConstructorsProduceTheFixtureValues(t *testing.T) {
	cases := map[string]ChildMessage{
		"child-ready":           NewReady(),
		"child-metric":          NewMetric("rps", 42),
		"child-metric-zero":     NewMetric("idle", 0),
		"child-action-reply":    NewReply("gc", "ok", nil),
		"child-action-reply-id": NewReply("gc", "ok", ptr(uint64(7))),
	}
	for name, built := range cases {
		if !reflect.DeepEqual(built, childCases[name]) {
			t.Fatalf("%s built %+v, want %+v", name, built, childCases[name])
		}
	}
}
