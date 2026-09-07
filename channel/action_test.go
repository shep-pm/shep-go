package channel

import (
	"reflect"
	"testing"
)

func TestFieldsSplitsParamsOnWhitespace(t *testing.T) {
	action := Action{Name: "gc", Params: ptr("  now   please ")}
	if got := action.Fields(); !reflect.DeepEqual(got, []string{"now", "please"}) {
		t.Fatalf("Fields is %#v", got)
	}
}

// An absent params and an empty one are different messages. Neither may
// panic here.
func TestFieldsIsEmptyForAbsentAndEmptyParams(t *testing.T) {
	if got := (Action{Name: "gc"}).Fields(); len(got) != 0 {
		t.Fatalf("Fields on absent params is %#v", got)
	}
	if got := (Action{Name: "gc", Params: ptr("")}).Fields(); len(got) != 0 {
		t.Fatalf("Fields on empty params is %#v", got)
	}
}

// The shepherd never reads params, so an app can pass JSON or anything
// carrying a space. Fields is a helper beside that, not a grammar on it.
func TestParamsSurvivesWhereFieldsWouldDestroyIt(t *testing.T) {
	action := Action{Name: "set", Params: ptr(`{"level":"debug and loud"}`)}
	if *action.Params != `{"level":"debug and loud"}` {
		t.Fatalf("Params is %q", *action.Params)
	}
}
