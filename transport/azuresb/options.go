package azuresb

import (
	"github.com/tx7do/kratos-transport/broker"
	"github.com/tx7do/kratos-transport/broker/azuresb"
)

type ServerOption func(o *Server)

// WithBrokerOptions sets broker options.
func WithBrokerOptions(opts ...broker.Option) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, opts...)
	}
}

// WithConnectionString sets the Azure Service Bus connection string.
func WithConnectionString(connStr string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, azuresb.WithConnectionString(connStr))
	}
}

// WithCodec sets the codec for message serialization.
func WithCodec(c string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, broker.WithCodec(c))
	}
}

// WithEnableKeepAlive enables or disables the keepalive server.
func WithEnableKeepAlive(enable bool) ServerOption {
	return func(s *Server) {
		s.enableKeepalive = enable
	}
}
