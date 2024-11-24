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
	NewReplicaOpcode
	MessageOpcode
	SyncOpcode
	SyncEchoOpcode
)

// Cluster main structure of consensus machine
type Cluster struct {
	clusterId byte
	replicaId uint32
	addr      string

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
	for id, addr := range cfg.Peers {
		s.AddPeer(id, addr)
	}

	return &Cluster{
		clusterId: cfg.ClusterId,
		replicaId: cfg.ReplicaId,
		addr:      cfg.Addr,
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

	peers := c.state.Peers()
	if len(peers) == 0 {
		return fmt.Errorf("cluster not ready yet, no peers")
	}
	c.broadcast(peers, &request{opcode: MessageOpcode, offsetId: msg.Id(), payload: msg.Checksum()})

	return nil
}

func (c *Cluster) broadcast(peers []sm.Peer, req *request) {
	var message = make([]byte, req.Length())
	message[0] = byte(req.opcode)
	message[1] = c.clusterId
	binary.BigEndian.PutUint32(message[2:6], c.replicaId)
	binary.BigEndian.PutUint32(message[6:10], req.offsetId)
	copy(message[10:], req.payload)

	c.log.Debug(fmt.Sprintf("Broadcast message payload=%v", message))

	for _, peer := range peers {
		c.wg.Add(1)
		go func(addr string, payload []byte) {
			defer c.wg.Done()

			err := c.out(addr, payload)
			if err != nil {
				c.log.Warn("Failed sending message", "err", err)
			}
		}(peer.Addr, message[:]) //todo: slice or array?
	}
}

func (c *Cluster) Close() error {
	close(c.stopCh)
	return nil
}
