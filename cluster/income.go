package cluster

func (c *Cluster) Income(message []byte) error {
	c.log.Info("Received message", "message", string(message))
	return nil
}
