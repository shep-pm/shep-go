// Command answers is a supervised app that answers on the shepherd
// channel.
//
// Run it under shep with `channel = true`. Then `shep trigger answers gc`
// reaches the handler below.
package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/shep-pm/shep-go/channel"
)

func main() {
	shepherd := channel.Serve()

	shepherd.OnAction("gc", func(a channel.Action) string {
		return "collected, fields=" + strings.Join(a.Fields(), ",")
	})
	shepherd.OnShutdown(func() {
		log.Print("the shepherd asked us to stop")
		os.Exit(0)
	})

	if err := shepherd.Ready(); err != nil {
		log.Fatalf("ready: %v", err)
	}
	log.Printf("channel active=%v stamp=%q", shepherd.Active(), shepherd.Version())

	for tick := 0; ; tick++ {
		shepherd.Metric("ticks", float64(tick))
		if dropped := shepherd.DroppedMetrics(); dropped > 0 {
			fmt.Println("dropped", dropped, "samples")
		}
		time.Sleep(time.Second)
	}
}
