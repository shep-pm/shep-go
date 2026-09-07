package channel

import "strings"

// Action is one action the shepherd dispatched to this app.
type Action struct {
	// Name is the action name the trigger asked for.
	Name string
	// Params is the argument text, nil when the trigger carried none.
	//
	// A pointer because the wire omits the key entirely: an absent
	// params and an empty one are different messages.
	Params *string
}

// Fields splits Params on whitespace, and is empty when there were none.
//
// The shepherd passes params through as one opaque string. An app can
// send JSON, or anything carrying a space. This is where a word split
// belongs, beside that string rather than in place of it.
func (a Action) Fields() []string {
	if a.Params == nil {
		return nil
	}
	return strings.Fields(*a.Params)
}
