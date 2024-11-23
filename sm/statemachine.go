package sm

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"
)

type StateMachine struct {
	db MessageDB
	m  map[uint32]*Message

	mux sync.RWMutex
}

func New(db MessageDB) *StateMachine {
	return &StateMachine{db: db, m: make(map[uint32]*Message)}
}

// Prepare - initiator accept request from client and create new message
func (sm *StateMachine) Prepare(message []byte) (*Message, error) {
	offset, err := sm.db.Offset()
	if err != nil {
		return nil, err
	}
	offset++
	msg := &Message{offset, message, checksum(message), PreparedState, time.Now()}
	sm.mux.Lock()
	sm.m[offset] = msg
	sm.mux.Unlock()
	return msg, nil
}

// Accept - initiator publish request to the other with information about new message
// Receiver create empty message. This means server accept the message and wait for the quorum
func (sm *StateMachine) Accept(id uint32, checksum []byte) error {
	sm.mux.RLock()
	_, ok := sm.m[id]
	sm.mux.RUnlock()
	if ok {
		return fmt.Errorf("message with already exists offset=%d", id)
	}

	sm.mux.Lock()
	sm.m[id] = &Message{id, nil, checksum, AcceptedState, time.Now()}
	sm.mux.Unlock()
	return nil
}

// Apply - receiver request message payload from initiator and commit it
func (sm *StateMachine) Apply(id uint32, message []byte) error {
	sm.mux.RLock()
	msg, ok := sm.m[id]
	sm.mux.RUnlock()
	if !ok {
		return fmt.Errorf("message not found offset=%d", id)
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

	msg.state = CommittedState

	err := sm.db.Store(msg)
	if err != nil {
		return err
	}

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

func checksum(src []byte) []byte {
	h := sha256.New()
	h.Write(src)
	return h.Sum(nil)
}
