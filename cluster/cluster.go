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
)

// Cluster main structure of consensus machine
type Cluster struct {
	id    uint32
	peers []string

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
	return &Cluster{
		id:    cfg.Id,
		out:   cfg.Out,
		peers: cfg.Peers,
		state: sm.New(cfg.DB),
		port:  cfg.Port,
		wg:    cfg.WaitGroup,
		log:   cfg.Logger,
	}
}

func (c *Cluster) Store(message []byte) error {
	msg, err := c.state.Prepare(message)
	if err != nil {
		return err
	}

	payload := make([]byte, 41)
	payload[0] = byte(MessageOpcode) // cmd "message"
	binary.BigEndian.PutUint32(payload[1:5], c.id)
	binary.BigEndian.PutUint32(payload[5:9], msg.Id())
	copy(payload[9:], msg.Checksum())

	c.log.Info(fmt.Sprintf("Broadcast message payload=%v", payload))
	c.broadcast(c.peers, payload)

	return nil
}

func (c *Cluster) broadcast(peers []string, payload []byte) {
	for _, peer := range peers {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			err := c.out(peer, payload)
			if err != nil {
				c.log.Warn("Failed sending message", "err", err)
			}
		}()
	}
}

func (c *Cluster) Close() error {
	close(c.stopCh)
	return nil
}
