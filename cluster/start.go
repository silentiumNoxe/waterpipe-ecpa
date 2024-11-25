package cluster

import (
	"context"
)

func (c *Cluster) Run(ctx context.Context) {
	c.heathbeat(ctx)
}
