package cluster

import (
	"context"
)

func (c *Cluster) Run(ctx context.Context) {
	stop := make(chan struct{})

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		<-ctx.Done()
		close(stop)
	}()

	go func() {
		c.log.Info("Start UDP server", "port", c.addr.Port)
		err := listen(c.wg, c.log, stop, c.addr, c.OnMessage)
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		c.heathbeat(stop)
	}()
}
