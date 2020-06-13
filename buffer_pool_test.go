package whispertool

import (
	"os"
	"testing"
)

func TestBufferPool(t *testing.T) {
	bufSize := os.Getpagesize()
	p := NewBufferPool(bufSize)
	b := p.Get()
	if got, want := len(b), bufSize; got != want {
		t.Errorf("unexpected fresh buffer size, got=%d, want=%d", got, want)
	}

	p.Put(b[:3])
	b = p.Get()
	if got, want := len(b), bufSize; got != want {
		t.Errorf("unexpected recycled buffer size, got=%d, want=%d", got, want)
	}
}
