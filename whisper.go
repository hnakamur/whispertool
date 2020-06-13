package whispertool

import (
	"bytes"
	"io"
	"math"
	"os"
	"sync"
	"unsafe"

	"github.com/hnakamur/timestamp"
)

type Whisper struct {
	Meta       Meta
	Retentions []Retention

	file *os.File
	buf  *bytes.Buffer
}

type Meta struct {
	AggType        uint32
	MaxRetention   uint32
	XFilesFactor   float32
	RetentionCount uint32
}

type Retention struct {
	Offset          uint32
	SecondsPerPoint uint32
	NumberOfPoints  uint32
}

type Point struct {
	Time  timestamp.Second
	Value float64
}

var uint32Size uintptr = unsafe.Sizeof(uint32(0))
var metaSize uintptr = unsafe.Sizeof(Meta{})
var retentionSize uintptr = unsafe.Sizeof(Retention{})

func Open(filename string) (*Whisper, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	w := &Whisper{file: file}
	if err := w.readFull(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Whisper) readFull() error {
	st, err := w.file.Stat()
	if err != nil {
		return err
	}

	w.buf = getBuf(int(st.Size()))
	if _, err := io.Copy(w.buf, w.file); err != nil {
		return err
	}

	w.Meta.AggType = w.uint32At(0)
	w.Meta.MaxRetention = w.uint32At(uint32Size)
	w.Meta.XFilesFactor = w.float32At(2 * uint32Size)
	w.Meta.RetentionCount = w.uint32At(3 * uint32Size)
	w.Retentions = make([]Retention, w.Meta.RetentionCount)
	for i := uint32(0); i < w.Meta.RetentionCount; i++ {
		w.Retentions[i] = w.retentionAt(metaSize + uintptr(i)*retentionSize)
	}

	return nil
}

func (w *Whisper) Fetch(retentionID int, from, until timestamp.Second) ([]Point, error) {
	return nil, nil
}

func (w *Whisper) uint32At(offset uintptr) uint32 {
	return ntohl(*(*uint32)(unsafe.Pointer(&w.buf.Bytes()[offset])))
}

func (w *Whisper) float32At(offset uintptr) float32 {
	return math.Float32frombits(w.uint32At(offset))
}

func (w *Whisper) retentionAt(offset uintptr) Retention {
	return Retention{
		Offset:          w.uint32At(offset),
		SecondsPerPoint: w.uint32At(offset + uint32Size),
		NumberOfPoints:  w.uint32At(offset + 2*uint32Size),
	}
}

func (w *Whisper) Close() error {
	if w.buf != nil {
		putBuf(w.buf)
	}
	return w.file.Close()
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func getBuf(size int) *bytes.Buffer {
	b := bufPool.Get().(*bytes.Buffer)
	b.Reset()
	b.Grow(size)
	return b
}

func putBuf(b *bytes.Buffer) {
	bufPool.Put(b)
}
