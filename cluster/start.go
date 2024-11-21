package cluster

import (
	"context"
	"io"
)

func (c *Cluster) Start(ctx context.Context) error {
	ctx2, cancel := context.WithCancel(ctx)

	c.wg.Add(1)
	go func() {
		c.wg.Add(1)
		go func(c *Cluster) {
			<-c.stopCh
			cancel()
		}(c)

		if err := listen(ctx2, c.wg, c.log, c.port, c.onmessage); err != nil {
			c.log.Warn("Error occurred", "err", err)
		}
	}()

	return nil
}

func (c *Cluster) onmessage(req *request, resp io.Writer) error {
	c.log.Info("Received message", "addr", req.Addr, "request", string(req.Payload))
	return nil
}
