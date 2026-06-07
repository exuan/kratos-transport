package temporal

import (
	"sync"

	"go.temporal.io/sdk/worker"

	"github.com/tx7do/kratos-transport/broker"
)

type subscriber struct {
	sync.RWMutex

	b       *temporalBroker
	worker  worker.Worker
	topic   string
	options broker.SubscribeOptions
	closed  bool
}

func (s *subscriber) Options() broker.SubscribeOptions {
	s.RLock()
	defer s.RUnlock()

	return s.options
}

func (s *subscriber) Topic() string {
	s.RLock()
	defer s.RUnlock()

	return s.topic
}

func (s *subscriber) Unsubscribe(removeFromManager bool) error {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	if s.worker != nil {
		s.worker.Stop()
	}

	if s.b != nil && s.b.subscribers != nil && removeFromManager {
		_ = s.b.subscribers.RemoveOnly(s.topic)
	}

	LogInfof("stopped temporal worker for task queue: %s", s.topic)

	return nil
}

func (s *subscriber) IsClosed() bool {
	s.RLock()
	defer s.RUnlock()

	return s.closed
}
