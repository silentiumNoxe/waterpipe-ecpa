package cluster

import (
	"encoding/binary"
	"fmt"
	"github.com/silentiumNoxe/goripple/sm"
	"log/slog"
	"sync"
	"time"
)

type Opcode byte

const (
	HeathbeatOpcode = iota + 1
	MessageOpcode
	SyncOpcode
	SyncEchoOpcode
)

// Cluster main structure of consensus machine
type Cluster struct {
	clusterId uint32
	serverId  uint32

	Addr string

	out Outcome

	state *sm.StateMachine

	wait time.Duration
	port string

	log    *slog.Logger
	wg     *sync.WaitGroup
	stopCh chan struct{}
}

func New(cfg *Config) *Cluster {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	s := sm.New(cfg.DB, cfg.Logger)

	return &Cluster{
		clusterId: cfg.ClusterId,
		serverId:  cfg.ServerId,
		Addr:      cfg.Addr,
		out:       cfg.Out,
		state:     s,
		port:      cfg.Port,
		wg:        cfg.WaitGroup,
		log:       cfg.Logger,
	}
}

func (c *Cluster) Store(message []byte) error {
	msg, err := c.state.Prepare(message)
	if err != nil {
		return err
	}

	payload := make([]byte, 36)
	binary.BigEndian.PutUint32(payload[0:5], msg.Id())
	copy(payload[9:], msg.Checksum())

	c.broadcast(c.state.Peers(), MessageOpcode, payload)

	return nil
}

func (c *Cluster) broadcast(peers []sm.Peer, opcode Opcode, payload []byte) {
	c.log.Debug(fmt.Sprintf("Broadcast message payload=%v", payload))

	for _, peer := range peers {
		c.wg.Add(1)
		go func(addr string, payload []byte) {
			defer c.wg.Done()

			var req = make([]byte, len(payload)+5)
			req[0] = byte(opcode)
			binary.BigEndian.PutUint32(req[1:5], c.clusterId)
			copy(req[5:], payload)

			err := c.out(addr, payload)
			if err != nil {
				c.log.Warn("Failed sending message", "err", err)
			}
		}(peer.Addr, payload)
	}
}

func (c *Cluster) Close() error {
	close(c.stopCh)
	return nil
}
