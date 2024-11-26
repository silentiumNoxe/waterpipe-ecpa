package cluster

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/silentiumNoxe/waterpipe-ecpa/sm"
)

type request struct {
	opcode    Opcode
	clusterId byte
	replicaId uint32
	offsetId  uint32
	payload   []byte
}

func (r request) Length() int {
	return len(r.payload) + 10 // 1 + 1 + 4 + 4
}

func (c *Cluster) OnMessage(message []byte) {
	var req = parse(message)

	if req.opcode == HeathbeatOpcode {
		if err := c.processHeathbeatOpcode(req); err != nil {
			c.log.Warn("Fail process message", "err", err)
		}
		return
	}
	if req.opcode == NewReplicaOpcode {
		if err := c.processNewReplicaOpcode(req); err != nil {
			c.log.Warn("Fail process message", "err", err)
		}
		return
	}
	if req.opcode == MessageOpcode {
		if err := c.processMessageOpcode(req); err != nil {
			c.log.Warn("Fail process message", "err", err)
		}
		return
	}
	if req.opcode == SyncOpcode {
		if err := c.processSyncOpcode(req); err != nil {
			c.log.Warn("Fail process message", "err", err)
		}
		return
	}
	if req.opcode == SyncEchoOpcode {
		if err := c.processSyncEchoOpcode(req); err != nil {
			c.log.Warn("Fail process message", "err", err)
		}
		return
	}

	c.log.Info(fmt.Sprintf("Unsupported opcode %d", req.opcode))
}

func parse(message []byte) *request {
	var r = request{}
	r.opcode = Opcode(message[0])
	r.clusterId = message[1]
	r.replicaId = binary.BigEndian.Uint32(message[2:6])
	r.offsetId = binary.BigEndian.Uint32(message[6:10])
	r.payload = message[10:]
	return &r
}

func (c *Cluster) processHeathbeatOpcode(req *request) error {
	addr := string(req.payload)
	if addr == "" {
		return fmt.Errorf("no address of replica")
	}

	isNew := c.state.AddPeer(req.replicaId, addr)
	if isNew {
		c.log.Info("Registered new peer", "replica", req.replicaId)

		peers := c.state.Peers()
		p := make([]sm.Peer, 0, len(peers))
		for _, peer := range peers {
			if peer.Id != req.replicaId {
				p = append(p, peer)
			}
		}
		var body = make([]byte, len(addr)+4)
		binary.BigEndian.PutUint32(body[:4], req.replicaId)
		copy(body[4:], addr)
		c.log.Info("Send new replica opcode")
		c.broadcast(p, &request{opcode: NewReplicaOpcode, payload: body})
	}
	return nil
}

func (c *Cluster) processNewReplicaOpcode(req *request) error {
	message := req.payload
	replicaId := binary.BigEndian.Uint32(message[:4])
	addr := string(message[4:])

	if replicaId == 0 {
		return fmt.Errorf("no replica id")
	}

	if addr == "" {
		return fmt.Errorf("no address of replica")
	}

	isNew := c.state.AddPeer(replicaId, addr)
	if isNew {
		c.log.Info("Registered new replica", "replica", replicaId)
	}
	return nil
}

func (c *Cluster) processMessageOpcode(req *request) error {
	checksum := req.payload

	if len(checksum) < 32 {
		return fmt.Errorf("wrong checksum size (%d)", len(checksum))
	}

	msg, err := c.state.Lookup(
		sm.LookupCriteria{
			OffsetIds: []uint32{req.offsetId},
		},
	)

	if err != nil {
		return err
	}

	if len(msg) > 1 {
		return fmt.Errorf("found two message with one offset clusterId %d", req.offsetId)
	}

	ready, err := c.state.Accept(req.offsetId, checksum)
	if err != nil {
		return nil
	}

	if ready {
		if msg[0].State() == sm.PreparedState {
			if err := c.state.Apply(req.offsetId, msg[0].Data()); err != nil {
				return err
			}
		} else if msg[0].State() == sm.AcceptedState {
			c.requestPayload(req.offsetId, msg[0].Checksum())
		}
	}

	if len(msg) == 0 {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.log.Info("Send message opcode echo", "offset", req.offsetId)
			c.broadcast(c.state.Peers(), req)
		}()
	}

	return nil
}

func (c *Cluster) requestPayload(offsetId uint32, checksum []byte) {
	c.log.Info("Send sync opcode", "offset", offsetId)
	c.broadcast(c.state.Peers(), &request{opcode: SyncOpcode, payload: checksum, offsetId: offsetId})
}

func (c *Cluster) processSyncOpcode(req *request) error {
	checksum := req.payload

	if len(checksum) < 32 {
		return fmt.Errorf("wrong checksum size (%d)", len(checksum))
	}

	msg, err := c.state.Lookup(
		sm.LookupCriteria{
			OffsetIds: []uint32{req.offsetId},
			Checksum:  checksum,
		},
	)
	if err != nil {
		return err
	}

	if len(msg) > 0 && msg[0].Data() != nil {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.log.Info("Send sync echo opcode", "offset", req.offsetId)
			c.broadcast(
				c.state.Peers(),
				&request{opcode: SyncEchoOpcode, offsetId: req.offsetId, payload: msg[0].Data()},
			)
		}()
	}

	if len(msg) == 0 {
		c.log.Info("Requested message not found", "offset", req.offsetId, "checksum", hex.EncodeToString(checksum))
	}

	return nil
}

func (c *Cluster) processSyncEchoOpcode(req *request) error {
	data := req.payload

	msg, err := c.state.Lookup(
		sm.LookupCriteria{
			OffsetIds: []uint32{req.offsetId},
		},
	)

	if err != nil {
		return err
	}

	if len(msg) == 0 {
		return nil
	}

	if msg[0].State() == sm.AcceptedState {
		c.log.Info("Apply message", "offset", req.offsetId)
		return c.state.Apply(req.offsetId, data)
	}

	return nil
}
