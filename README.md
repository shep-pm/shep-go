# shep-go

Go client for [shep](https://github.com/shep-pm/shep). Signal readiness,
emit a metric, answer an action, all over the channel shep opens for a
supervised app.

```go
import "github.com/shep-pm/shep-go/channel"

shepherd := channel.Serve()

shepherd.OnAction("gc", func(a channel.Action) string {
	runtime.GC()
	return "collected"
})
shepherd.OnShutdown(server.GracefulStop)
if err := shepherd.Ready(); err != nil {
	log.Print(err)
}
shepherd.Metric("rps", 4200)
```

```
go get github.com/shep-pm/shep-go/channel
```

Ask for a channel in the Flockfile with `channel = true`, or get one from
`wait_ready` or `shutdown_with_message`.

Without one, every call above does nothing and the app runs unchanged, so
there is nothing to branch on. An action name you never registered still
gets a reply, which is the part an app is most likely to get wrong: to the
shepherd, silence from an app thinking hard and silence from an app that
never understood the question look the same, and only `action_timeout`
running out tells them apart.

`channel.Open()` is the layer underneath, for an app that already runs its
own event loop: a `Conn` with `Recv`, `Send` and no goroutines. Both go
through the same door, so pick one and never both.

Standard library only. The dog client lands at 0.2.0 as its own module.

Dual licensed under MIT or Apache-2.0.
