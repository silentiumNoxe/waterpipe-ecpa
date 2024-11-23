package sm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"
)

type StateMachine struct {
	db    MessageDB
	m     map[uint32]*Message
	peers []string

	wt       time.Duration
	expStack int
	mux      sync.RWMutex
	log      *slog.Logger
}

func New(db MessageDB, peers []string, wt time.Duration, expStack int, log *slog.Logger) *StateMachine {
	return &StateMachine{db: db, m: make(map[uint32]*Message), peers: peers, wt: wt, expStack: expStack, log: log}
}

func (sm *StateMachine) Monitor(ctx context.Context) {
	var stop = false
	go func() {
		<-ctx.Done()
		stop = true
	}()

	for {
		if stop {
			return
		}

		var i = 0
		var arr = make([]*Message, sm.expStack)

		var exp = time.Now().Add(-sm.wt)

		sm.mux.RLock()
		for _, msg := range sm.m {
			if msg.timestamp.After(exp) {
				arr[i] = msg
			}
		}
		sm.mux.RUnlock()

		if len(arr) == 0 {
			continue
		}

		sm.mux.Lock()
		for _, msg := range arr {
			delete(sm.m, msg.id)
		}
		sm.mux.Unlock()
	}
}

// Prepare - initiator accept request from client and create new message
func (sm *StateMachine) Prepare(message []byte) (*Message, error) {
	offset, err := sm.db.Offset()
	if err != nil {
		return nil, err
	}
	offset++
	msg := &Message{offset, message, checksum(message), PreparedState, time.Now(), 0}
	sm.mux.Lock()
	sm.m[offset] = msg
	sm.mux.Unlock()
	return msg, nil
}

// Accept - initiator publish request to the other with information about new message
// Receiver create empty message. This means server accept the message and wait for the quorum
// returns bool value that describes that we ready for commit
func (sm *StateMachine) Accept(id uint32, checksum []byte) (bool, error) {
	sm.mux.RLock()
	msg, ok := sm.m[id]
	sm.mux.RUnlock()
	if ok {
		msg.quorum++
		sm.log.Info(
			"[SM] Increase quorum counter",
			"offset",
			id,
			"quorum",
			fmt.Sprintf("%d/%d", msg.quorum, sm.Quorum()),
		)
		return msg.quorum >= sm.Quorum(), nil
	}

	sm.log.Info("[SM] Accept message", "offset", id, "quorum", fmt.Sprintf("%d/%d", 1, sm.Quorum()))
	sm.mux.Lock()
	sm.m[id] = &Message{id, nil, checksum, AcceptedState, time.Now(), 1}
	sm.mux.Unlock()
	return false, nil
}

// Apply - receiver request message payload from initiator and commit it
func (sm *StateMachine) Apply(id uint32, message []byte) error {
	sm.mux.RLock()
	msg, ok := sm.m[id]
	sm.mux.RUnlock()
	if !ok {
		return fmt.Errorf("message not found offset=%d", id)
	}

	if msg.quorum < sm.Quorum() {
		return fmt.Errorf(
			"wrong state of message, quorum less then required offset=%s",
			fmt.Sprintf("%d/%d", msg.quorum, sm.Quorum()),
		)
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
	sm.mux.RLock()
	msg, ok := sm.m[id]
	sm.mux.RUnlock()
	if !ok {
		return fmt.Errorf("message not found offset=%d", id)
	}

	if msg.quorum < sm.Quorum() {
		return fmt.Errorf(
			"wrong state of message, quorum less then required offset=%s",
			fmt.Sprintf("%d/%d", msg.quorum, sm.Quorum()),
		)
	}

	sm.log.Info("[SM] Commit message", "offset", id)
	msg.state = CommittedState
	err := sm.db.Update(id, msg)
	if err != nil {
		return err
	}

	delete(sm.m, id)

	return nil
}

func (sm *StateMachine) Lookup(criteria LookupCriteria) ([]*Message, error) {
	arr, err := sm.db.Lookup(criteria)
	if err != nil {
		return nil, err
	}

	sm.mux.RLock()
	for offset, msg := range sm.m {
		var skip = true
		for _, a := range criteria.OffsetIds {
			if offset == a {
				skip = false
			}
		}

		for _, state := range criteria.States {
			if msg.state == state {
				skip = false
			}
		}

		after := !criteria.After.IsZero()
		before := !criteria.Before.IsZero()

		if after && before {
			if msg.timestamp.After(criteria.After) && msg.timestamp.Before(criteria.Before) {
				skip = false
			}
		} else if after {
			if msg.timestamp.After(criteria.After) {
				skip = false
			}
		} else if before {
			if msg.timestamp.Before(criteria.Before) {
				skip = false
			}
		}

		if criteria.Checksum != nil {
			c := msg.Checksum()
			if len(criteria.Checksum) != len(msg.Checksum()) {
				skip = true
			} else {
				for i, b := range criteria.Checksum {
					if c[i] != b {
						skip = true
						break
					}
				}
			}
		}

		if skip == false {
			arr = append(arr, msg)
		}
	}
	sm.mux.RUnlock()

	return arr, nil
}

func (sm *StateMachine) Peers() []string {
	return sm.peers
}

func (sm *StateMachine) Quorum() int {
	return int(math.Ceil(float64(len(sm.peers)+1) / float64(2)))
}

func checksum(src []byte) []byte {
	h := sha256.New()
	h.Write(src)
	return h.Sum(nil)
}
