package sm

import (
	"time"
)

type Peer struct {
	Id            uint32
	Addr          string
	LastHeathbeat time.Time
}
