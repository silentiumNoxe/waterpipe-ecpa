package cluster

import (
	"github.com/silentiumNoxe/goripple/sm"
	"log/slog"
	"sync"
	"time"
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
		state: sm.New(cfg.db),
		port:  cfg.Port,
		wg:    cfg.WaitGroup,
		log:   cfg.Logger,
	}
}

func (c *Cluster) Store(message []byte) error {
	c.broadcast(c.peers, message)
	//reg, err := c.sm.Register(message)
	//if err != nil {
	//	return fmt.Errorf("failed register message: %w", err)
	//}
	//
	//for _, peer := range c.peers {
	//	c.wg.Add(1)
	//	go func(reg *goripple.Message) {
	//		defer c.wg.Done()
	//		payload := make([]byte, 41)
	//		payload[0] = 1 // cmd "message"
	//		binary.LittleEndian.AppendUint32(payload, c.id)
	//		binary.LittleEndian.AppendUint32(payload, reg.Id())
	//		payload = append(payload, reg.Checksum()...)
	//		if err := c.pub.Message(peer, payload); err != nil {
	//			c.log.Warn(
	//				"failed to send message to peer",
	//				"peer", peer,
	//				"err", err,
	//			)
	//		}
	//	}(reg)
	//}
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
