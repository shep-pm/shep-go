package channel

// ptr returns a pointer to v.
//
// Every optional field on the wire is a pointer. omitempty then drops an
// absent value and never a zero one.
func ptr[T any](v T) *T { return &v }

// NewReady builds the readiness signal.
func NewReady() ChildMessage {
	return ChildMessage{Kind: KindReady}
}

// NewMetric builds one metric sample.
func NewMetric(name string, value float64) ChildMessage {
	return ChildMessage{Kind: KindMetric, Name: ptr(name), Value: ptr(value)}
}

// NewReply builds the answer to one action.
//
// id is echoed verbatim, including its absence. The shepherd matches a
// reply by id, and falls back to name and order without one.
func NewReply(action, body string, id *uint64) ChildMessage {
	return ChildMessage{Kind: KindActionReply, Action: ptr(action), Body: ptr(body), ID: id}
}
