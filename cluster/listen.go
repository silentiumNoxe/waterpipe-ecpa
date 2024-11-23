package cluster

import (
	"encoding/binary"
	"fmt"
	"github.com/silentiumNoxe/goripple/sm"
)

func (c *Cluster) OnMessage(addr string, message []byte) error {
	c.log.Info(fmt.Sprintf("Received message %v", message), "addr", addr, "length", len(message))
	opcode := Opcode(message[0])
	if opcode == MessageOpcode {
		return c.processMessage(message)
	}

	return fmt.Errorf("unsupported opcode %d", opcode)
}

func (c *Cluster) processMessage(message []byte) error {
	clusterId := binary.BigEndian.Uint32(message[1:5])
	offsetId := binary.BigEndian.Uint32(message[5:9])
	checksum := message[9:]

	if clusterId != c.id {
		return fmt.Errorf("wrong cluster id %d", clusterId)
	}

	if len(checksum) < 32 {
		return fmt.Errorf("wrong checksum size")
	}

	msg, err := c.state.Lookup(
		sm.LookupCriteria{
			States:    []sm.State{sm.PreparedState},
			OffsetIds: []uint32{offsetId},
			Checksum:  checksum,
		},
	)

	if err != nil {
		return err
	}

	if len(msg) > 1 {
		return fmt.Errorf("found two message with one offset id %d", offsetId)
	}

	if len(msg) > 0 {
		return c.state.Commit(offsetId)
	}

	err = c.state.Accept(offsetId, checksum)
	if err != nil {
		return err
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.broadcast(c.peers, message)
	}()

	return nil
}
