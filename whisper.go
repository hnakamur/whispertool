package whispertool

import (
	"encoding/binary"
	"io"
	"log"
	"math"
	"os"

	"github.com/hnakamur/timestamp"
)

type pageID uint64

type Whisper struct {
	Meta       Meta
	Retentions []Retention

	bufPool  *BufferPool
	pageSize int64

	file         *os.File
	fileSize     int64
	lastPageID   pageID
	lastPageSize int64

	pages map[pageID][]byte

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

// These are sizes in whisper files.
// NOTE: The size of type Point is different from the size of
// point in file since timestamp.Seconds is int64, not uint32.
const (
	uint32Size    = 4
	uint64Size    = 8
	float32Size   = 4
	float64Size   = 8
	metaSize      = 3*uint32Size + float32Size
	retentionSize = 3 * uint32Size
	pointSize     = uint32Size + float64Size
)

func Open(filename string, bufPool *BufferPool) (*Whisper, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	st, err := file.Stat()
	if err != nil {
		return nil, err
	}

	pageSize := int64(bufPool.BufferSize())
	fileSize := st.Size()
	lastPageID := pageID(fileSize / pageSize)
	lastPageSize := fileSize % pageSize

	w := &Whisper{
		file:         file,
		bufPool:      bufPool,
		pageSize:     pageSize,
		fileSize:     fileSize,
		lastPageID:   lastPageID,
		lastPageSize: lastPageSize,
		pages:        make(map[pageID][]byte),
	}
	if err := w.readMeta(); err != nil {
		return nil, err
	}
	if err := w.readRetentions(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Whisper) Close() error {
	for _, page := range w.pages {
		w.bufPool.Put(page)
	}
	return w.file.Close()
}

func (w *Whisper) readMeta() error {
	if err := w.readPagesIfNeeded(0, 0); err != nil {
		return err
	}

	w.Meta.AggType = w.uint32At(0)
	w.Meta.MaxRetention = w.uint32At(uint32Size)
	w.Meta.XFilesFactor = w.float32At(2 * uint32Size)
	w.Meta.RetentionCount = w.uint32At(3 * uint32Size)
	return nil
}

func (w *Whisper) readRetentions() error {
	nRet := int64(w.Meta.RetentionCount)
	off := metaSize + nRet*retentionSize
	until := pageID(off / w.pageSize)
	if err := w.readPagesIfNeeded(0, until); err != nil {
		return err
	}

	w.Retentions = make([]Retention, nRet)
	for i := int64(0); i < nRet; i++ {
		off := metaSize + i*retentionSize
		w.Retentions[i] = w.retentionAt(off)
	}
	return nil
}

func (w *Whisper) readPagesIfNeeded(from, until pageID) error {
	log.Printf("readPagesIfNeeded start, from=%d, until=%d", from, until)
	for from <= until {
		for from <= until {
			if _, ok := w.pages[from]; ok {
				from++
			} else {
				break
			}
		}
		if from > until {
			return nil
		}

		iovs := [][]byte{w.allocBuf(from)}
		pid := from + 1
		for pid <= until {
			if _, ok := w.pages[pid]; !ok {
				iovs = append(iovs, w.allocBuf(pid))
				pid++
			} else {
				break
			}
		}

		off := int64(from) * w.pageSize
		log.Printf("readPagesIfNeeded readv from=%d, to=%d", from, pid-1)
		_, err := preadvFull(w.file, iovs, off)
		if err != nil {
			return err
		}

		for ; from < pid; from++ {
			w.pages[from] = iovs[0]
			iovs = iovs[1:]
		}
	}
	return nil
}

func (w *Whisper) allocBuf(pid pageID) []byte {
	b := w.bufPool.Get()
	if pid == w.lastPageID {
		b = b[:w.lastPageSize]
	}
	return b
}

func (w *Whisper) pageAt(id pageID) ([]byte, error) {
	p := w.pages[id]
	if p != nil {
		return p, nil
	}

	b := w.bufPool.Get()
	off := int64(id) * int64(w.bufPool.BufferSize())
	n, err := w.file.ReadAt(b, off)
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
		b = b[:n]
	}
	w.pages[id] = b
	return b, nil
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
	off := int64(r.Offset)
	p := make([]Point, r.NumberOfPoints)
	for i := uint32(0); i < r.NumberOfPoints; i++ {
		p[i].Time = w.timestampAt(off)
		p[i].Value = w.float64At(off + uint32Size)
		off += pointSize
	}
	return p
}

func (w *Whisper) uint32At(offset int64) uint32 {
	buf := w.bufForValue(offset, uint32Size)
	return binary.BigEndian.Uint32(buf)
}

func (w *Whisper) uint64At(offset int64) uint64 {
	buf := w.bufForValue(offset, uint64Size)
	return binary.BigEndian.Uint64(buf)
}

func (w *Whisper) float32At(offset int64) float32 {
	return math.Float32frombits(w.uint32At(offset))
}

func (w *Whisper) float64At(offset int64) float64 {
	return math.Float64frombits(w.uint64At(offset))
}

func (w *Whisper) bufForValue(offset, size int64) []byte {
	pgID := pageID(offset / w.pageSize)
	offInPg := offset % w.pageSize
	overflow := offInPg + size - w.pageSize
	if overflow <= 0 {
		p := w.pages[pgID]
		return p[offInPg : offInPg+size]
	}

	buf := make([]byte, size)
	p0 := w.pages[pgID]
	p1 := w.pages[pgID+1]
	copy(buf, p0[offInPg:])
	copy(buf[offInPg:], p1[:overflow])
	return buf
}

func (w *Whisper) retentionAt(offset int64) Retention {
	return Retention{
		Offset:          w.uint32At(offset),
		SecondsPerPoint: w.uint32At(offset + uint32Size),
		NumberOfPoints:  w.uint32At(offset + 2*uint32Size),
	}
}

func (w *Whisper) timestampAt(offset int64) timestamp.Second {
	return timestamp.Second(w.uint32At(offset))
}

func (w *Whisper) baseInterval(r *Retention) timestamp.Second {
	return w.timestampAt(int64(r.Offset))
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
