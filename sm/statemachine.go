package sm

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"time"
)

type StateMachine struct {
	db MessageDB
	m  map[uint32]*Message
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
	sm.m[offset] = msg
	return msg, nil
}

// Accept - initiator publish request to the other with information about new message
// Receiver create empty message. This means server accept the message and wait for the quorum
func (sm *StateMachine) Accept(id uint32, checksum []byte) error {
	_, ok := sm.m[id]
	if ok {
		return fmt.Errorf("message with already exists offset=%d", id)
	}

	sm.m[id] = &Message{id, nil, checksum, AcceptedState, time.Now()}
	return nil
}

// Apply - receiver request message payload from initiator and commit it
func (sm *StateMachine) Apply(id uint32, message []byte) error {
	msg, ok := sm.m[id]
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
	msg, ok := sm.m[id]
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

func checksum(src []byte) []byte {
	return sha256.New().Sum(src)
}
