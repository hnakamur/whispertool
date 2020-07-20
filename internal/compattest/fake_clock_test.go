package compattest

import (
	"sync"
	"time"
)

type fakeClock struct {
	now time.Time
	mu  sync.Mutex
}

func (c *fakeClock) SetFixed(t time.Time) {
	c.mu.Lock()
	c.now = t
	c.mu.Unlock()
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
