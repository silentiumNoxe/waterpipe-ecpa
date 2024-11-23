package cluster

import (
	"context"
	"encoding/binary"
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
			var req = make([]byte, len(c.Addr)+4)
			binary.BigEndian.PutUint32(req[0:4], c.serverId)
			copy(req[4:], c.Addr)
			c.log.Debug("Send heathbeat opcode")
			c.broadcast(c.state.Peers(), HeathbeatOpcode, req)
		}
	}()
}
