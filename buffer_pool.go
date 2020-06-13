package whispertool

import (
	"sync"
	"unsafe"
)

type BufferPool struct {
	bufferSize int
	pool       sync.Pool
}

func NewBufferPool(bufferSize int) *BufferPool {
	if uintptr(bufferSize) < unsafe.Sizeof(uint64(0)) {
		panic("bufferSize must not be smaller than 8")
	}
	return &BufferPool{
		bufferSize: bufferSize,
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, bufferSize)
			},
		},
	}
}

func (p *BufferPool) BufferSize() int {
	return p.bufferSize
}

func (p *BufferPool) Get() []byte {
	return p.pool.Get().([]byte)
}

func (p *BufferPool) Put(b []byte) {
	if cap(b) != p.bufferSize {
		panic("wrong buffer capacity")
	}
	b = b[:cap(b)]
	for i := range b {
		b[i] = 0
	}
	p.pool.Put(b)
}
