package sm

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type StateMachine struct {
	db    MessageDB
	m     map[uint32]*Message
	peers map[uint32]Peer

	mux     sync.RWMutex
	peerMux sync.RWMutex
	log     *slog.Logger
}

func New(db MessageDB, log *slog.Logger) *StateMachine {
	return &StateMachine{db: db, m: make(map[uint32]*Message), peers: make(map[uint32]Peer), log: log}
}

// Prepare - initiator accept request from client and create new message
func (sm *StateMachine) Prepare(message []byte) (*Message, error) {
	offset, err := sm.db.Offset()
	if err != nil {
		return nil, err
	}
	offset++
	msg := &Message{offset, message, checksum(message), PreparedState, time.Now(), new(atomic.Uint32)}
	sm.mux.Lock()
	sm.m[offset] = msg
	sm.mux.Unlock()
	return msg, nil
}

// Accept - initiator publish request to the other with information about new message
// Receiver create empty message. This means server accept the message and wait for the quorum
// returns bool value that describes that we ready for commit
func (sm *StateMachine) Accept(id uint32, checksum []byte) (bool, error) {
	sm.mux.RLock()
	msg, ok := sm.m[id]
	sm.mux.RUnlock()
	if ok {
		var qv = msg.quorum.Add(1)
		sm.log.Debug(
			"[SM] Increase quorum counter",
			"offset",
			id,
			"quorum",
			fmt.Sprintf("%d/%d", qv, sm.Quorum()),
		)
		return qv >= sm.Quorum(), nil
	}

	sm.log.Debug("[SM] Accept message", "offset", id, "quorum", fmt.Sprintf("%d/%d", 1, sm.Quorum()))
	sm.mux.Lock()
	var qv = new(atomic.Uint32)
	qv.Store(1)
	sm.m[id] = &Message{id, nil, checksum, AcceptedState, time.Now(), qv}
	sm.mux.Unlock()
	return false, nil
}

// Apply - receiver request message payload from initiator and commit it
func (sm *StateMachine) Apply(id uint32, message []byte) error {
	sm.mux.RLock()
	msg, ok := sm.m[id]
	sm.mux.RUnlock()
	if !ok {
		return fmt.Errorf("message not found offset=%d", id)
	}

	if msg.quorum.Load() < sm.Quorum() {
		return fmt.Errorf(
			"wrong state of message, quorum less then required offset=%s",
			fmt.Sprintf("%d/%d", msg.quorum, sm.Quorum()),
		)
	}

	if bytes.Compare(checksum(message), msg.checksum) != 0 {
		return fmt.Errorf("checksum mismatch offset=%d", id)
	}

	msg.data = message
	err := sm.db.Store(msg)
	if err != nil {
		return err
	}

	return sm.Commit(id)
}

// Commit - store message to the message db
func (sm *StateMachine) Commit(id uint32) error {
	sm.mux.RLock()
	msg, ok := sm.m[id]
	sm.mux.RUnlock()
	if !ok {
		return fmt.Errorf("message not found offset=%d", id)
	}

	if msg.quorum.Load() < sm.Quorum() {
		return fmt.Errorf(
			"wrong state of message, quorum less then required offset=%s",
			fmt.Sprintf("%d/%d", msg.quorum, sm.Quorum()),
		)
	}

	sm.log.Debug("[SM] Commit message", "offset", id)
	msg.state = CommittedState
	err := sm.db.Update(id, msg)
	if err != nil {
		return err
	}

	delete(sm.m, id)

	return nil
}

func (sm *StateMachine) Lookup(criteria LookupCriteria) ([]*Message, error) {
	arr, err := sm.db.Lookup(criteria)
	if err != nil {
		return nil, err
	}

	sm.mux.RLock()
	for offset, msg := range sm.m {
		var skip = true
		for _, a := range criteria.OffsetIds {
			if offset == a {
				skip = false
			}
		}

		for _, state := range criteria.States {
			if msg.state == state {
				skip = false
			}
		}

		after := !criteria.After.IsZero()
		before := !criteria.Before.IsZero()

		if after && before {
			if msg.timestamp.After(criteria.After) && msg.timestamp.Before(criteria.Before) {
				skip = false
			}
		} else if after {
			if msg.timestamp.After(criteria.After) {
				skip = false
			}
		} else if before {
			if msg.timestamp.Before(criteria.Before) {
				skip = false
			}
		}

		if criteria.Checksum != nil {
			c := msg.Checksum()
			if len(criteria.Checksum) != len(msg.Checksum()) {
				skip = true
			} else {
				for i, b := range criteria.Checksum {
					if c[i] != b {
						skip = true
						break
					}
				}
			}
		}

		if skip == false {
			arr = append(arr, msg)
		}
	}
	sm.mux.RUnlock()

	return arr, nil
}

func (sm *StateMachine) Peers() []Peer {
	sm.peerMux.RLock()
	defer sm.peerMux.RUnlock()

	l := len(sm.peers)
	arr := make([]Peer, l)

	i := 0
	for _, peer := range sm.peers {
		arr[i] = peer
		i++
	}
	return arr
}

// AddPeer returns boolean value that describes is a peer new
func (sm *StateMachine) AddPeer(id uint32, addr string) bool {
	sm.peerMux.Lock()
	defer sm.peerMux.Unlock()

	p, ok := sm.peers[id]
	if !ok {
		sm.peers[id] = Peer{Id: id, Addr: addr, LastHeathbeat: time.Now()}
		return true
	}
	p.Addr = addr
	p.LastHeathbeat = time.Now()
	return false
}

func (sm *StateMachine) Quorum() uint32 {
	return uint32(math.Ceil(float64(len(sm.peers)+1) / float64(2)))
}

func checksum(src []byte) []byte {
	h := sha256.New()
	h.Write(src)
	return h.Sum(nil)
}

func Checksum(src []byte) []byte {
	return checksum(src)
}
