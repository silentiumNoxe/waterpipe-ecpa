package cluster

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/silentiumNoxe/waterpipe-ecpa/sm"
	"sort"
)

type request struct {
	id           uint64
	opcode       Opcode
	clusterId    byte
	otp          uint32
	replicaId    uint32
	segment      uint32
	segmentCount uint32
	payload      []byte
}

func (r request) Length() int {
	return len(r.payload)
}

// here I should wait for all segments. When all segments arrived then we can process it

var cache = make(map[uint64][]*request)

func cacheRequest(req *request) *request {
	r, ok := cache[req.id]
	if !ok {
		r = make([]*request, 0, req.segmentCount)
	}

	r = append(r, req)
	cache[req.id] = r

	if uint32(len(r)) == req.segmentCount {
		delete(cache, req.id)
		return merge(r)
	}

	return nil
}

func merge(arr []*request) *request {
	if arr == nil || len(arr) == 0 {
		return nil
	}

	sort.Slice(
		arr, func(i, j int) bool {
			return arr[i].segment < arr[j].segment
		},
	)

	var req = &request{
		id:           arr[0].id,
		opcode:       arr[0].opcode,
		clusterId:    arr[0].clusterId,
		otp:          arr[0].otp,
		replicaId:    arr[0].replicaId,
		segment:      arr[0].segment,
		segmentCount: arr[0].segmentCount,
	}

	var payload = bytes.Buffer{}

	for _, seg := range arr {
		payload.Write(seg.payload)
	}

	req.payload = payload.Bytes()

	return req
}

func auth(otp uint32, secret []byte) error {
	gen, err := genTOTP(secret, 20, 8)
	if err != nil {
		return fmt.Errorf("unable to generate otp: %w", err)
	}

	if gen != otp {
		return fmt.Errorf("wrong otp")
	}

	return nil
}

func (c *Cluster) OnMessage(message []byte) {
	c.log.Info(fmt.Sprintf("Received message %v", message))
	req, err := parse(message)
	if err != nil {
		c.log.Warn("Unable to parse message", "err", err)
		return
	}

	if err := auth(req.otp, c.secret); err != nil {
		c.log.Warn("Failed authorization", "err", err)
		return
	}

	if req.segmentCount > 1 {
		full := cacheRequest(req)
		if full == nil {
			return
		}

		req = full
	}

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
			c.log.Warn("Fail process message", "err", err, "opcode", req.opcode)
		}
		return
	}
	if req.opcode == SyncOpcode {
		if err := c.processSyncOpcode(req); err != nil {
			c.log.Warn("Fail process message", "err", err, "opcode", req.opcode)
		}
		return
	}
	if req.opcode == SyncEchoOpcode {
		if err := c.processSyncEchoOpcode(req); err != nil {
			c.log.Warn("Fail process message", "err", err, "opcode", req.opcode)
		}
		return
	}

	c.log.Info(fmt.Sprintf("Unsupported opcode %d", req.opcode))
}

func parse(message []byte) (*request, error) {
	var head = message[:64]
	var data = message[64:]

	var headR = bytes.NewReader(head)

	opcode, err := headR.ReadByte()
	if err != nil {
		return nil, err
	}

	cluster, err := headR.ReadByte()
	if err != nil {
		return nil, err
	}

	var otp uint32
	err = binary.Read(headR, binary.BigEndian, &otp)
	if err != nil {
		return nil, err
	}

	var replica uint32
	err = binary.Read(headR, binary.BigEndian, &replica)
	if err != nil {
		return nil, err
	}

	var id uint64
	err = binary.Read(headR, binary.BigEndian, &id)
	if err != nil {
		return nil, err
	}

	var segment uint32
	err = binary.Read(headR, binary.BigEndian, &segment)
	if err != nil {
		return nil, err
	}

	var segmentCount uint32
	err = binary.Read(headR, binary.BigEndian, &segmentCount)
	if err != nil {
		return nil, err
	}

	return &request{
		id:           id,
		opcode:       Opcode(opcode),
		clusterId:    cluster,
		otp:          otp,
		replicaId:    replica,
		segment:      segment,
		segmentCount: segmentCount,
		payload:      data,
	}, nil
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

		otp, err := genTOTP(c.secret, 20, 8)
		if err != nil {
			return fmt.Errorf("unable to generate otp: %w", err)
		}

		c.log.Info("Send new replica opcode")
		c.broadcast(p, &request{opcode: NewReplicaOpcode, payload: body}, otp)
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
	var offset = binary.BigEndian.Uint32(req.payload[:4])
	if offset == 0 {
		return fmt.Errorf("no offset")
	}

	checksum := req.payload[4:]

	if len(checksum) < 32 {
		return fmt.Errorf("wrong checksum size (%d)", len(checksum))
	}

	msg, err := c.state.Lookup(
		sm.LookupCriteria{
			OffsetIds: []uint32{offset},
		},
	)

	if err != nil {
		return fmt.Errorf("unable to lookup message: %w", err)
	}

	if len(msg) > 1 {
		return fmt.Errorf("found two message with one offset clusterId %d", offset)
	}

	ready, err := c.state.Accept(offset, checksum)
	if err != nil {
		return fmt.Errorf("unable to accept message: %w", err)
	}

	if ready {
		if msg[0].State() == sm.PreparedState {
			if err := c.state.Apply(offset, msg[0].Data()); err != nil {
				return fmt.Errorf("unable to apply message: %w", err)
			}
		} else if msg[0].State() == sm.AcceptedState {
			if err := c.requestPayload(offset, msg[0].Checksum()); err != nil {
				return err
			}
		}
	}

	if len(msg) == 0 {
		otp, err := genTOTP(c.secret, 20, 8)
		if err != nil {
			return fmt.Errorf("unable to generate otp: %w", err)
		}

		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.log.Info("Send message opcode echo", "offset", offset)
			c.broadcast(c.state.Peers(), req, otp)
		}()
	}

	return nil
}

func (c *Cluster) requestPayload(offsetId uint32, checksum []byte) error {
	otp, err := genTOTP(c.secret, 20, 8)
	if err != nil {
		return fmt.Errorf("unable to generate otp: %w", err)
	}

	var payload = new(bytes.Buffer)
	if err := binary.Write(payload, binary.BigEndian, offsetId); err != nil {
		return err
	}
	payload.Write(checksum)

	c.log.Info("Send sync opcode", "offset", offsetId)
	c.broadcast(c.state.Peers(), &request{opcode: SyncOpcode, payload: payload.Bytes()}, otp)

	return nil
}

func (c *Cluster) processSyncOpcode(req *request) error {
	var offset = binary.BigEndian.Uint32(req.payload[:4])
	if offset == 0 {
		return fmt.Errorf("no offset")
	}

	checksum := req.payload[4:]

	if len(checksum) < 32 {
		return fmt.Errorf("wrong checksum size (%d)", len(checksum))
	}

	msg, err := c.state.Lookup(
		sm.LookupCriteria{
			OffsetIds: []uint32{offset},
			Checksum:  checksum,
		},
	)
	if err != nil {
		return err
	}

	if len(msg) > 0 && msg[0].Data() != nil {
		var payload = new(bytes.Buffer)
		if err := binary.Write(payload, binary.BigEndian, offset); err != nil {
			return err
		}

		payload.Write(msg[0].Data())

		otp, err := genTOTP(c.secret, 20, 8)
		if err != nil {
			return fmt.Errorf("unable to generate otp: %w", err)
		}

		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.log.Info("Send sync echo opcode", "offset", offset)
			c.broadcast(
				c.state.Peers(),
				&request{opcode: SyncEchoOpcode, payload: payload.Bytes()},
				otp,
			)
		}()
	}

	if len(msg) == 0 {
		c.log.Info("Requested message not found", "offset", offset, "checksum", hex.EncodeToString(checksum))
	}

	return nil
}

func (c *Cluster) processSyncEchoOpcode(req *request) error {
	var offset = binary.BigEndian.Uint32(req.payload[:4])
	if offset == 0 {
		return fmt.Errorf("no offset")
	}

	data := req.payload[4:]

	msg, err := c.state.Lookup(
		sm.LookupCriteria{
			OffsetIds: []uint32{offset},
		},
	)

	if err != nil {
		return err
	}

	if len(msg) == 0 {
		return nil
	}

	if msg[0].State() == sm.AcceptedState {
		c.log.Info("Apply message", "offset", offset)
		return c.state.Apply(offset, data)
	}

	return nil
}
