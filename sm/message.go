package sm

import (
	"time"
)

type Message struct {
	id        uint32
	data      []byte
	checksum  []byte
	state     State
	timestamp time.Time
}

func NewMessage(id uint32, data []byte, checksum []byte, state State, timestamp time.Time) Message {
	return Message{id, data, checksum, state, timestamp}
}

func (m Message) Id() uint32 {
	return m.id
}

func (m Message) Data() []byte {
	return m.data
}

func (m Message) Checksum() []byte {
	return m.checksum
}

func (m Message) State() State {
	return m.state
}

func (m Message) Timestamp() time.Time {
	return m.timestamp
}

func (m Message) IsCommitted() bool {
	return m.state == CommittedState
}

type State byte

const (
	PreparedState = iota + 1
	AcceptedState
	CommittedState
)
