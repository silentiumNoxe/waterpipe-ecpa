package cluster

import (
	"fmt"
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
		otp, err := genTOTP(c.secret, 20, 8)
		if err != nil {
			panic(fmt.Errorf("unable to generate otp: %w", err))
		}

		c.broadcast(c.state.Peers(), &request{opcode: HeathbeatOpcode, payload: []byte(c.addr.String())}, otp)
		time.Sleep(time.Second * 10)
	}
}
