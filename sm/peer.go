package sm

import (
	"time"
)

type Peer struct {
	Id            string
	Addr          string
	LastHeathbeat time.Time
}
