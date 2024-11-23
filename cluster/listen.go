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
		return c.processMessageOpcode(message)
	}

	return fmt.Errorf("unsupported opcode %d", opcode)
}

func (c *Cluster) processMessageOpcode(message []byte) error {
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

	ready, err := c.state.Accept(offsetId, checksum)
	if err != nil {
		return nil
	}

	if ready {
		if msg[0].State() == sm.PreparedState {
			if err := c.state.Commit(offsetId); err != nil {
				return err
			}
		} else if msg[0].State() == sm.AcceptedState {
			//todo: request message payload
			if err := c.state.Apply(offsetId, []byte("TESTTEST")); err != nil {
				return err
			}
		}
	}

	if len(msg) == 0 {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.broadcast(c.state.Peers(), message)
		}()
	}

	return nil
}
