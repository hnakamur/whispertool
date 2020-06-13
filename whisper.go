package whispertool

import (
	"bytes"
	"encoding/binary"
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

	file          *os.File
	buf           *bytes.Buffer
	baseIntervals []timestamp.Second
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
var pointSize uintptr = unsafe.Sizeof(Point{})

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

func (w *Whisper) Close() error {
	if w.buf != nil {
		putBuf(w.buf)
	}
	return w.file.Close()
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

func (w *Whisper) FetchFromArchive(retentionID int, from, until timestamp.Second) ([]Point, error) {
	//now := timestamp.FromTimeToSecond(time.Now())

	//r := w.Retentions[retentionID]
	//tNow := r.alignTime(now)
	//if now < until {
	//	until = now
	//} else {
	//	until = r.alignTime(until)
	//}

	//tMin := tNow - timestamp.Second(r.NumberOfPoints*r.SecondsPerPoint)
	//if from < tMin {
	//	from = tMin
	//} else {
	//	from = r.alignTime(from)
	//}

	return nil, nil
}

func (w *Whisper) GetRawPoints(retentionID int) []Point {
	r := w.Retentions[retentionID]
	off := uintptr(r.Offset)
	p := make([]Point, r.NumberOfPoints)
	for i := uint32(0); i < r.NumberOfPoints; i++ {
		p[i].Time = w.timestampAt(off)
		p[i].Value = w.float64At(off + uint32Size)
		off += pointSize
	}
	return p
}

func (w *Whisper) uint32At(offset uintptr) uint32 {
	return binary.BigEndian.Uint32(w.buf.Bytes()[offset:])
}

func (w *Whisper) uint64At(offset uintptr) uint64 {
	return binary.BigEndian.Uint64(w.buf.Bytes()[offset:])
}

func (w *Whisper) float32At(offset uintptr) float32 {
	return math.Float32frombits(w.uint32At(offset))
}

func (w *Whisper) float64At(offset uintptr) float64 {
	return math.Float64frombits(w.uint64At(offset))
}

func (w *Whisper) retentionAt(offset uintptr) Retention {
	return Retention{
		Offset:          w.uint32At(offset),
		SecondsPerPoint: w.uint32At(offset + uint32Size),
		NumberOfPoints:  w.uint32At(offset + 2*uint32Size),
	}
}

func (w *Whisper) timestampAt(offset uintptr) timestamp.Second {
	return timestamp.Second(w.uint32At(offset))
}

func (w *Whisper) baseInterval(r *Retention) timestamp.Second {
	return w.timestampAt(uintptr(r.Offset))
}

func (r *Retention) pointIndex(baseInterval, interval timestamp.Second) int {
	pointDistance := int64(interval-baseInterval) / int64(r.SecondsPerPoint)
	return int(floorMod(pointDistance, int64(r.NumberOfPoints)))
}

func (r *Retention) pointOffsetAt(index int) uintptr {
	return uintptr(r.Offset) + uintptr(index)*pointSize
}

func (r *Retention) Interval(t timestamp.Second) timestamp.Second {
	step := int64(r.SecondsPerPoint)
	return timestamp.Second(int64(t) - floorMod(int64(t), step) + step)
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func getBuf(size int) *bytes.Buffer {
	b := bufPool.Get().(*bytes.Buffer)
	b.Reset()
	if b.Len() < size {
		b.Grow(size - b.Len())
	}
	return b
}

func putBuf(b *bytes.Buffer) {
	bufPool.Put(b)
}
