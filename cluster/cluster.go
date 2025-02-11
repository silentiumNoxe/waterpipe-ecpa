package cluster

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"github.com/silentiumNoxe/waterpipe-ecpa/sm"
	"log/slog"
	"math"
	"net"
	"sync"
	"time"
)

type Opcode byte

const (
	HeathbeatOpcode = iota + 1
	NewReplicaOpcode
	MessageOpcode
	SyncOpcode
	SyncEchoOpcode
)

// Cluster main structure of consensus machine
type Cluster struct {
	clusterId byte
	replicaId uint32
	addr      *net.UDPAddr

	state *sm.StateMachine

	log    *slog.Logger
	wg     *sync.WaitGroup
	stopCh chan struct{}

	secret []byte
}

func (c *Cluster) SyncStore(ctx context.Context, message []byte) (uint32, error) {
	if _, ok := ctx.Deadline(); ok {
		return 0, fmt.Errorf("deadline not set")
	}

	off, err := c.AsyncStore(message)
	if err != nil {
		return off, err
	}

	t := time.NewTicker(time.Millisecond * 30)
	defer t.Stop()

	criteria := sm.LookupCriteria{OffsetIds: []uint32{off}}

	for {
		select {
		case <-t.C:
			result, err := c.state.Lookup(criteria)
			if err != nil {
				slog.Warn("Unable to lookup message", slog.String("err", err.Error()))
				continue
			}

			if len(result) == 0 {
				return off, fmt.Errorf("message not found")
			}

			if result[0].IsCommitted() {
				return off, nil
			}
		case <-ctx.Done():
			err = c.Rollback(off)
			if err != nil {
				return off, err
			}
		}
	}
}

func (c *Cluster) AsyncStore(message []byte) (uint32, error) {
	msg, err := c.state.Prepare(message)
	if err != nil {
		return 0, err
	}

	peers := c.state.Peers()
	if len(peers) == 0 {
		return msg.Id(), fmt.Errorf("cluster not ready yet, no peers")
	}

	otp, err := genTOTP(c.secret, 20, 8)
	if err != nil {
		return msg.Id(), fmt.Errorf("unable to generate otp: %w", err)
	}

	c.broadcast(peers, &request{opcode: MessageOpcode, offsetId: msg.Id(), payload: msg.Checksum()}, otp)

	return msg.Id(), nil
}

func (c *Cluster) Rollback(offsetId uint32) error {
	//todo: implement me
	panic("implement me")
}

func (c *Cluster) broadcast(peers []sm.Peer, req *request, otp uint32) {
	var message = make([]byte, req.Length()+1+1+4+4+4)
	message[0] = byte(req.opcode)
	message[1] = c.clusterId
	binary.BigEndian.PutUint32(message[2:6], otp)
	binary.BigEndian.PutUint32(message[6:10], c.replicaId)
	binary.BigEndian.PutUint32(message[10:14], req.offsetId)
	copy(message[14:], req.payload)

	for _, peer := range peers {
		c.wg.Add(1)

		go func(addr string, payload []byte) {
			defer c.wg.Done()

			err := publish(addr, payload)
			if err != nil {
				c.log.Warn("Failed sending message", "err", err)
			}
		}(peer.Addr, message[:]) //todo: slice or array?
	}
}

func genTOTP(secret []byte, exp int64, size int) (uint32, error) {
	h := hmac.New(sha1.New, secret)
	counter := time.Now().Unix() / exp
	if err := binary.Write(h, binary.BigEndian, counter); err != nil {
		return 0, err
	}

	hash := h.Sum(nil)
	offset := hash[len(hash)-1] & 0xf
	code := ((uint32(hash[offset]&0x7f) << 24) |
		(uint32(hash[offset+1]) << 16) |
		(uint32(hash[offset+2]) << 8) |
		uint32(hash[offset+3])) % uint32(math.Pow10(size))

	return code, nil
}

func (c *Cluster) Close() error {
	close(c.stopCh)
	return nil
}
