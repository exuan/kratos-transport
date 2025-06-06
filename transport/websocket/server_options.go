package websocket

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"github.com/go-kratos/kratos/v2/encoding"
)

type PayloadType uint8

const (
	PayloadTypeBinary = 0
	PayloadTypeText   = 1
)

type ServerOption func(o *Server)

func WithNetwork(network string) ServerOption {
	return func(s *Server) {
		if network != "" {
			s.network = network
		}
	}
}

func WithAddress(addr string) ServerOption {
	return func(s *Server) {
		if addr != "" {
			s.address = addr
		}
	}
}

func WithStrictSlash(strictSlash bool) ServerOption {
	return func(s *Server) {
		s.strictSlash = strictSlash
	}
}

func WithPath(path string) ServerOption {
	return func(s *Server) {
		if path != "" {
			s.path = path
		}
	}
}

func WithTLSConfig(c *tls.Config) ServerOption {
	return func(o *Server) {
		o.tlsConf = c
	}
}

func WithListener(lis net.Listener) ServerOption {
	return func(s *Server) {
		s.lis = lis
	}
}

func WithCodec(c string) ServerOption {
	return func(s *Server) {
		if c != "" {
			s.codec = encoding.GetCodec(c)
		}
	}
}

func WithReadBufferSize(size int) ServerOption {
	return func(s *Server) {
		s.upgrader.ReadBufferSize = size
	}
}

func WithWriteBufferSize(size int) ServerOption {
	return func(s *Server) {
		s.upgrader.WriteBufferSize = size
	}
}

func WithCheckOrigin(domain string) ServerOption {
	return func(s *Server) {
		s.upgrader.CheckOrigin = func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			return origin == domain
		}
	}
}

func WithEnableCompression(enable bool) ServerOption {
	return func(s *Server) {
		s.upgrader.EnableCompression = enable
	}
}

func WithHandshakeTimeout(timeout time.Duration) ServerOption {
	return func(s *Server) {
		s.upgrader.HandshakeTimeout = timeout
	}
}

func WithChannelBufferSize(size int) ServerOption {
	return func(_ *Server) {
		channelBufSize = size
	}
}

func WithPayloadType(payloadType PayloadType) ServerOption {
	return func(s *Server) {
		s.payloadType = payloadType
	}
}

func WithInjectTokenToQuery(enable bool, tokenKey string) ServerOption {
	return func(s *Server) {
		s.injectToken = enable
		s.tokenKey = tokenKey
	}
}

func WithMessageMarshaler(m NetPacketMarshaler) ServerOption {
	return func(s *Server) {
		s.netPacketMarshaler = m
	}
}

func WithMessageUnmarshaler(m NetPacketUnmarshaler) ServerOption {
	return func(s *Server) {
		s.netPacketUnmarshaler = m
	}
}

func WithSocketConnectHandler(h SocketConnectHandler) ServerOption {
	return func(s *Server) {
		s.sessionMgr.RegisterConnectHandler(h)
	}
}

func WithSocketRawDataHandler(h SocketRawDataHandler) ServerOption {
	return func(s *Server) {
		if h != nil {
			s.socketRawDataHandler = h
		}
	}
}
