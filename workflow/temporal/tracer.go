package temporal

import (
	"go.opentelemetry.io/otel/propagation"
)

var _ propagation.TextMapCarrier = (*mapCarrier)(nil)

type mapCarrier struct {
	m map[string]string
}

func newMapCarrier() *mapCarrier {
	return &mapCarrier{m: make(map[string]string)}
}

func (c *mapCarrier) Get(key string) string {
	return c.m[key]
}

func (c *mapCarrier) Set(key, val string) {
	c.m[key] = val
}

func (c *mapCarrier) Keys() []string {
	keys := make([]string, 0, len(c.m))
	for k := range c.m {
		keys = append(keys, k)
	}
	return keys
}
