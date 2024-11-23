package cluster

import (
	"github.com/silentiumNoxe/goripple/sm"
	"log/slog"
	"sync"
)

type Config struct {
	Id  uint32
	Out Outcome
	db  sm.MessageDB

	Peers     []string
	Port      string
	WaitGroup *sync.WaitGroup
	Logger    *slog.Logger
}
