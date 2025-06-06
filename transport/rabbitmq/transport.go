package rabbitmq

import (
	"github.com/go-kratos/kratos/v2/selector"
	kratosTransport "github.com/go-kratos/kratos/v2/transport"
)

const (
	KindRabbitMQ = "rabbitmq"
)

var _ kratosTransport.Transporter = &Transport{}

// Transport is a RabbitMQ transport.
type Transport struct {
	endpoint    string
	operation   string
	reqHeader   headerCarrier
	replyHeader headerCarrier
	nodeFilters []selector.NodeFilter
}

// Kind returns the transport kind.
func (tr *Transport) Kind() kratosTransport.Kind {
	return KindRabbitMQ
}

// Endpoint returns the transport endpoint.
func (tr *Transport) Endpoint() string {
	return tr.endpoint
}

// Operation returns the transport operation.
func (tr *Transport) Operation() string {
	return tr.operation
}

// RequestHeader returns the request header.
func (tr *Transport) RequestHeader() kratosTransport.Header {
	return tr.reqHeader
}

// ReplyHeader returns the reply header.
func (tr *Transport) ReplyHeader() kratosTransport.Header {
	return tr.replyHeader
}

// NodeFilters returns the client select filters.
func (tr *Transport) NodeFilters() []selector.NodeFilter {
	return tr.nodeFilters
}

type headerCarrier struct{}

// Get returns the value associated with the passed key.
func (hc headerCarrier) Get(_ string) string {
	return ""
}

// Set stores the key-value pair.
func (hc headerCarrier) Set(_ string, _ string) {

}

// Keys lists the keys stored in this carrier.
func (hc headerCarrier) Keys() []string {
	return nil
}

// Add append value to key-values pair.
func (hc headerCarrier) Add(_ string, _ string) {

}

// Values returns a slice of values associated with the passed key.
func (hc headerCarrier) Values(_ string) []string {
	return nil
}
