package cluster

import (
	"github.com/silentiumNoxe/goripple/trans"
	"log/slog"
	"sync"
)

type Config struct {
	Id        uint32
	Transport trans.Transport

	Peers     []string
	Port      string
	WaitGroup *sync.WaitGroup
	Logger    *slog.Logger
}
