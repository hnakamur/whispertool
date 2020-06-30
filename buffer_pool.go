package whispertool

import (
	"sync"
	"unsafe"
)

type bufferPool struct {
	bufferSize int
	pool       sync.Pool
}

func newBufferPool(bufferSize int) *bufferPool {
	if uintptr(bufferSize) < unsafe.Sizeof(uint64(0)) {
		panic("bufferSize must not be smaller than 8")
	}
	return &bufferPool{
		bufferSize: bufferSize,
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, bufferSize)
			},
		},
	}
}

func (p *bufferPool) BufferSize() int {
	return p.bufferSize
}

func (p *bufferPool) Get() []byte {
	return p.pool.Get().([]byte)
}

func (p *bufferPool) Put(b []byte) {
	if cap(b) != p.bufferSize {
		panic("wrong buffer capacity")
	}
	b = b[:cap(b)]
	for i := range b {
		b[i] = 0
	}
	p.pool.Put(b)
}
