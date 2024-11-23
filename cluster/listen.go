package cluster

import (
	"encoding/binary"
	"fmt"
	"github.com/silentiumNoxe/goripple/sm"
)

func (c *Cluster) OnMessage(addr string, message []byte) error {
	c.log.Info(fmt.Sprintf("Received message %v", message), "addr", addr, "length", len(message))
	opcode := Opcode(message[0])
	clusterId := binary.BigEndian.Uint32(message[1:5])
	if c.id != clusterId {
		return fmt.Errorf("invalid cluster id")
	}

	if opcode == MessageOpcode {
		return c.processMessageOpcode(message[5:])
	}
	if opcode == SyncOpcode {
		return c.processSyncOpcode(message[5:])
	}

	return fmt.Errorf("unsupported opcode %d", opcode)
}

func (c *Cluster) processMessageOpcode(message []byte) error {
	offsetId := binary.BigEndian.Uint32(message[4:8])
	checksum := message[8:]

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
			c.requestPayload(offsetId, msg[0].Checksum())
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

func (c *Cluster) requestPayload(offsetId uint32, checksum []byte) {
	var request = make([]byte, 41)
	request[0] = SyncOpcode
	binary.BigEndian.PutUint32(request[1:5], c.id)
	binary.BigEndian.PutUint32(request[5:9], offsetId)
	copy(request, checksum)
	c.broadcast(c.state.Peers(), request)
}

func (c *Cluster) processSyncOpcode(message []byte) error {
	offsetId := binary.BigEndian.Uint32(message[4:8])
	checksum := message[8:]

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

	if len(msg) > 1 && msg[0].Data() != nil {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			var req = make([]byte, 9+len(msg[0].Data()))
			req[0] = SyncEchoOpcode
			binary.BigEndian.PutUint32(req[1:5], c.id)
			binary.BigEndian.PutUint32(req[5:9], offsetId)
			copy(req[9:], msg[0].Data())
			c.broadcast(c.state.Peers(), message)
		}()
	}

	return nil
}

func (c *Cluster) processSyncEchoOpcode(message []byte) error {
	offsetId := binary.BigEndian.Uint32(message[4:8])
	data := message[8:]

	msg, err := c.state.Lookup(
		sm.LookupCriteria{
			OffsetIds: []uint32{offsetId},
		},
	)

	if err != nil {
		return err
	}

	if len(msg) == 0 {
		return nil
	}

	if msg[0].State() == sm.AcceptedState {
		c.log.Info("Apply message", "offset", offsetId)
		return c.state.Apply(offsetId, data)
	}

	return nil
}
