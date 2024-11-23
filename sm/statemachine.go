package sm

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type StateMachine struct {
	db    MessageDB
	m     map[uint32]*Message
	peers []string

	mux sync.RWMutex
	log *slog.Logger
}

func New(db MessageDB, peers []string, log *slog.Logger) *StateMachine {
	return &StateMachine{db: db, m: make(map[uint32]*Message), peers: peers}
}

// Prepare - initiator accept request from client and create new message
func (sm *StateMachine) Prepare(message []byte) (*Message, error) {
	offset, err := sm.db.Offset()
	if err != nil {
		return nil, err
	}
	offset++
	msg := &Message{offset, message, checksum(message), PreparedState, time.Now(), 1}
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
		msg.quorum++
		sm.log.Info(
			"[SM] Increase quorum counter",
			"offset",
			id,
			"quorum",
			fmt.Sprintf("%d/%d", msg.quorum, sm.Quorum()),
		)
		return msg.quorum > sm.Quorum(), nil
	}

	sm.log.Info("[SM] Accept message", "offset", id)
	sm.mux.Lock()
	sm.m[id] = &Message{id, nil, checksum, AcceptedState, time.Now(), 1}
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

	if msg.quorum < sm.Quorum() {
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

	if msg.quorum < sm.Quorum() {
		return fmt.Errorf(
			"wrong state of message, quorum less then required offset=%s",
			fmt.Sprintf("%d/%d", msg.quorum, sm.Quorum()),
		)
	}

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

		if criteria.Checksum != nil && bytes.Compare(msg.checksum, criteria.Checksum) == 0 {
			skip = false
		}

		if skip == false {
			arr = append(arr, msg)
		}
	}
	sm.mux.RUnlock()

	return arr, nil
}

func (sm *StateMachine) Peers() []string {
	return sm.peers
}

func (sm *StateMachine) Quorum() int {
	return len(sm.peers) / 2
}

func checksum(src []byte) []byte {
	h := sha256.New()
	h.Write(src)
	return h.Sum(nil)
}
