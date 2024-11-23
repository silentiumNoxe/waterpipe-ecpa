package cluster

import (
	"github.com/silentiumNoxe/goripple/sm"
	"log/slog"
	"sync"
	"time"
)

type Config struct {
	Id          uint32
	Out         Outcome
	DB          sm.MessageDB
	WaitTimeout time.Duration

	Peers     []string
	Port      string
	WaitGroup *sync.WaitGroup
	Logger    *slog.Logger
}
