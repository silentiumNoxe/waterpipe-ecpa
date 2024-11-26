package cluster

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
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

	out Outcome

	state *sm.StateMachine

	wait time.Duration
	port string

	log    *slog.Logger
	wg     *sync.WaitGroup
	stopCh chan struct{}

	secret []byte
}

func New(cfg *Config) *Cluster {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	s := sm.New(cfg.DB, cfg.Logger)
	for id, addr := range cfg.Peers {
		s.AddPeer(id, addr)
	}

	secret, err := hex.DecodeString(cfg.Secret)
	if err != nil {
		panic(fmt.Errorf("Failed to decode secret: %w ", err))
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
		secret:    secret,
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

	otp, err := genTOTP(c.secret, 20, 8)
	if err != nil {
		return fmt.Errorf("unable to generate otp: %w", err)
	}

	c.broadcast(peers, &request{opcode: MessageOpcode, offsetId: msg.Id(), payload: msg.Checksum()}, otp)

	return nil
}

func (c *Cluster) broadcast(peers []sm.Peer, req *request, otp uint32) {
	var message = make([]byte, req.Length())
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
