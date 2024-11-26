package cluster

import (
	"time"
)

func (c *Cluster) heathbeat(stop <-chan struct{}) {
	var stopped = false
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		<-stop
		stopped = true
	}()

	for !stopped {
		c.log.Info("Send heathbeat")
		c.broadcast(c.state.Peers(), &request{opcode: HeathbeatOpcode, payload: []byte(c.addr.String())})
		time.Sleep(time.Second * 10)
	}
}
