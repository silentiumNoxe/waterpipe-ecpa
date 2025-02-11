package sm

import (
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
)

type Message struct {
	id        uint32
	data      []byte
	checksum  []byte
	state     State
	timestamp time.Time
	quorum    *atomic.Uint32
}

func NewMessage(id uint32, data []byte, checksum []byte, state State, timestamp time.Time) Message {
	return Message{id, data, checksum, state, timestamp, &atomic.Uint32{}}
}

func (m *Message) Id() uint32 {
	return m.id
}

func (m *Message) Data() []byte {
	return m.data
}

func (m *Message) Checksum() []byte {
	return m.checksum
}

func (m *Message) State() State {
	return m.state
}

func (m *Message) Timestamp() time.Time {
	return m.timestamp
}

func (m *Message) IsCommitted() bool {
	return m.state == CommittedState
}

func (m *Message) String() string {
	return fmt.Sprintf(
		"{offset: %d, data: %s, checksum: %s, state: %d, timestamp: %v}",
		m.id,
		m.data,
		hex.EncodeToString(m.checksum),
		m.state,
		m.timestamp,
	)
}

type State byte

const (
	PreparedState State = iota + 1
	AcceptedState
	CommittedState
)
