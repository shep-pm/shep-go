// Package channel speaks the shep shepherd channel: signal readiness,
// emit a metric, answer an action.
//
// Serve is the documented default. Its handle answers an action nobody
// registered. Without a channel every method does nothing, so no call
// site needs a branch. Open is the layer under it, for an app that
// drives its own loop. The contract is docs/shepherd-channel.md in the
// shep repository.
package channel
