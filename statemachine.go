package goripple

import (
	"crypto/sha256"
)

type StateMachine interface {
	Register(message []byte) (*Message, error)

	Accept(id uint32) error
	Commit(id uint32) error

	FindById(id uint32) (*Message, error)
	FindByChecksum(sum string) (*Message, error)
}

type Message struct {
	id      uint32
	payload []byte

	accepted bool
	commited bool
}

func (m Message) Checksum() []byte {
	return sha256.New().Sum(m.payload)
}

func (m Message) Id() uint32 {
	return m.id
}

func (m Message) Payload() []byte {
	return m.payload
}
