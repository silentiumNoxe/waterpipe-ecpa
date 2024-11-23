package sm

import (
	"time"
)

type MessageDB interface {
	Offset() (uint32, error)

	Store(message *Message) error
	Update(id uint32, message *Message) error
	Drop(id uint32) error

	Lookup(criteria LookupCriteria) ([]*Message, error)
}

type LookupCriteria struct {
	OffsetIds []uint32
	States    []State
	Checksum  []byte
	Before    time.Time
	After     time.Time
}
