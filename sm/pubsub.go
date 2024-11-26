package sm

type PubSub[T any] struct {
	subscribers []func(T)
}

func (ps *PubSub[T]) SyncPub(val T) {
	for _, sub := range ps.subscribers {
		sub(val)
	}
}

func (ps *PubSub[T]) AsyncPub(val T) {
	go ps.SyncPub(val)
}

func (ps *PubSub[T]) Sub(f func(T)) {
	ps.subscribers = append(ps.subscribers, f)
}

func (ps *PubSub[T]) Unsubscribe(f func(T)) {

}
