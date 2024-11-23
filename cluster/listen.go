package cluster

import (
	"encoding/binary"
	"fmt"
)

func (c *Cluster) OnMessage(addr string, message []byte) error {
	opcode := Opcode(message[0])
	if opcode == MessageOpcode {
		return c.processMessage(message)
	}

	return fmt.Errorf("unsupported opcode %d", opcode)
}

func (c *Cluster) processMessage(message []byte) error {
	clusterId := binary.LittleEndian.Uint32(message[1:5])
	offsetId := binary.LittleEndian.Uint32(message[5:9])
	checksum := message[9:41]

	if clusterId != c.id {
		return fmt.Errorf("wrong cluster id %d", clusterId)
	}

	if len(checksum) < 32 {
		return fmt.Errorf("wrong checksum size")
	}

	return c.state.Accept(offsetId, checksum)
}
