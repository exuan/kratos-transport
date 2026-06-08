package gcpubsub

import (
	"github.com/tx7do/kratos-transport/broker"
	"github.com/tx7do/kratos-transport/broker/gcpubsub"
)

type ServerOption func(o *Server)

// WithBrokerOptions sets broker options.
func WithBrokerOptions(opts ...broker.Option) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, opts...)
	}
}

// WithProjectID sets the GCP project ID.
func WithProjectID(projectID string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, gcpubsub.WithProjectID(projectID))
	}
}

// WithCredentialsFile sets the path to a service account credentials JSON file.
func WithCredentialsFile(path string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, gcpubsub.WithCredentialsFile(path))
	}
}

// WithEndpoint sets a custom endpoint (for testing with emulators).
func WithEndpoint(endpoint string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, gcpubsub.WithEndpoint(endpoint))
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
