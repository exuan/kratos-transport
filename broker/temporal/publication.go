package temporal

import (
	"github.com/tx7do/kratos-transport/broker"
)

type publication struct {
	m     *broker.Message
	topic string
	err   error
}

func (p *publication) Ack() error {
	// Temporal workflows auto-ack; no manual ACK needed.
	return nil
}

func (p *publication) Error() error {
	return p.err
}

func (p *publication) Topic() string {
	return p.topic
}

func (p *publication) Message() *broker.Message {
	return p.m
}

func (p *publication) RawMessage() any {
	return p.m
}
