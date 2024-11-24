package cluster

import (
	"context"
	"time"
)

func (c *Cluster) heathbeat(ctx context.Context) {
	var stop = false
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		<-ctx.Done()
		stop = true
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for !stop {
			c.log.Debug("Send heathbeat opcode")
			c.broadcast(c.state.Peers(), &request{opcode: HeathbeatOpcode, payload: []byte(c.addr)})
			time.Sleep(time.Second * 10)
		}
	}()
}
