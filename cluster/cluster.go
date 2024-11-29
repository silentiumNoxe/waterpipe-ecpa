package cluster

import (
	"bytes"
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

	var payload = &bytes.Buffer{}
	err = binary.Write(payload, binary.BigEndian, msg.Id())
	if err != nil {
		return err
	}
	payload.Write(msg.Checksum())

	c.broadcast(peers, &request{opcode: MessageOpcode, payload: payload.Bytes()}, otp)

	return nil
}

// broadcast send to each peer the request. Max payload size 512 bytes, if payload will be more than
// broadcast will split payload to segments and send it separately
func (c *Cluster) broadcast(peers []sm.Peer, req *request, otp uint32) {
	const maxSize = 512
	const headerSize = 64
	const payloadSize = maxSize - headerSize

	var segmentsCount uint32 = 1

	if len(req.payload) > payloadSize {
		segmentsCount = uint32(math.Ceil(float64(len(req.payload)) / float64(payloadSize)))
	}

	var segments = make([][]byte, 0, segmentsCount)

	var r = bytes.NewReader(req.payload)
	var id = time.Now().UnixMilli()

	for i := 0; true; i++ {
		var meta = bytes.NewBuffer(make([]byte, 64))
		meta.WriteByte(byte(req.opcode))
		meta.WriteByte(c.clusterId)
		_ = binary.Write(meta, binary.BigEndian, otp)
		_ = binary.Write(meta, binary.BigEndian, c.replicaId)
		_ = binary.Write(meta, binary.BigEndian, uint64(id))
		_ = binary.Write(meta, binary.BigEndian, uint32(i))
		_ = binary.Write(meta, binary.BigEndian, segmentsCount)

		var data = make([]byte, payloadSize)
		n, err := r.Read(data)
		if err != nil {
			c.log.Warn("Failed to read payload", "err", err)
			return
		}

		if n == 0 {
			break
		}

		var msg = make([]byte, maxSize)
		copy(msg[:headerSize], meta.Bytes())
		copy(msg[headerSize:], data)

		segments = append(segments, msg)

		if n < payloadSize {
			break
		}
	}

	for _, peer := range peers {
		for i, seg := range segments {
			if err := publish(peer.Addr, seg); err != nil {
				c.log.Warn("Failed sending message", "err", err, "segment", i)
			}
		}
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
