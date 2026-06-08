package sqs

import (
	"github.com/tx7do/kratos-transport/broker"
	"github.com/tx7do/kratos-transport/broker/sqs"
)

type ServerOption func(o *Server)

// WithBrokerOptions sets broker options.
func WithBrokerOptions(opts ...broker.Option) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, opts...)
	}
}

// WithRegion sets the AWS region.
func WithRegion(region string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, sqs.WithRegion(region))
	}
}

// WithEndpoint sets a custom endpoint URL (for local testing with ElasticMQ/LocalStack).
func WithEndpoint(endpoint string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, sqs.WithEndpoint(endpoint))
	}
}

// WithQueueUrl sets the default queue URL.
func WithQueueUrl(url string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, sqs.WithQueueUrl(url))
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
