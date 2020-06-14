package whispertool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"time"

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

var ErrRetentionIDOutOfRange = errors.New("retention is ID out of range")

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
	w.initBaseIntervals()
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

func (w *Whisper) initBaseIntervals() {
	w.baseIntervals = make([]timestamp.Second, len(w.Retentions))
	for i := range w.baseIntervals {
		w.baseIntervals[i] = -1
	}
}

func (w *Whisper) baseInterval(retentionID int) (timestamp.Second, error) {
	interval := w.baseIntervals[retentionID]
	if interval != -1 {
		return interval, nil
	}

	r := w.Retentions[retentionID]
	off := int64(r.Offset)
	fromPg := pageID(off / w.pageSize)
	untilPg := pageID((off + uint32Size) / w.pageSize)
	if err := w.readPagesIfNeeded(fromPg, untilPg); err != nil {
		return 0, fmt.Errorf("cannot read page from %d to %d for baseInterval: %s",
			fromPg, untilPg, err)
	}
	return w.timestampAt(off), nil
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

func (w *Whisper) FetchFromSpecifiedArchive(retentionID int, from, until timestamp.Second) ([]Point, error) {
	now := timestamp.FromTimeToSecond(time.Now())
	log.Printf("FetchFromSpecifiedArchive start, from=%s, until=%s, now=%s",
		formatTime(secondsToTime(int64(from))),
		formatTime(secondsToTime(int64(until))),
		formatTime(secondsToTime(int64(now))))
	if from > until {
		return nil, fmt.Errorf("invalid time interval: from time '%d' is after until time '%d'", from, until)
	}
	oldest := now - timestamp.Second(w.Meta.MaxRetention)
	// range is in the future
	if from > now {
		return nil, nil
	}
	// range is beyond retention
	if until < oldest {
		return nil, nil
	}
	if from < oldest {
		from = oldest
	}
	if until > now {
		until = now
	}
	log.Printf("FetchFromSpecifiedArchive adjusted, from=%s, until=%s, now=%s",
		formatTime(secondsToTime(int64(from))),
		formatTime(secondsToTime(int64(until))),
		formatTime(secondsToTime(int64(now))))

	if retentionID < 0 || len(w.Retentions)-1 < retentionID {
		return nil, ErrRetentionIDOutOfRange
	}

	baseInterval, err := w.baseInterval(retentionID)
	log.Printf("FetchFromSpecifiedArchive baseInterval=%s, err=%v",
		formatTime(secondsToTime(int64(baseInterval))), err)
	if err != nil {
		return nil, err
	}

	r := &w.Retentions[retentionID]
	fromInterval := r.Interval(from)
	untilInterval := r.Interval(until)
	step := timestamp.Second(r.SecondsPerPoint)

	if baseInterval == 0 {
		points := make([]Point, (untilInterval-fromInterval)/step)
		t := fromInterval
		for i := range points {
			points[i].Time = t
			points[i].Value = math.NaN()
			t += step
		}
		return points, nil
	}

	// Zero-length time range: always include the next point
	if fromInterval == untilInterval {
		untilInterval += step
	}

	points, err := w.fetchRawPoints(fromInterval, untilInterval, retentionID)
	if err != nil {
		return nil, err
	}
	for i, pt := range points {
		log.Printf("rawPoint i=%d, time=%s, value=%s",
			i,
			formatTime(secondsToTime(int64(pt.Time))),
			strconv.FormatFloat(pt.Value, 'f', -1, 64))
	}
	clearOldPoints(points, fromInterval, step)

	return points, nil
}

func (w *Whisper) fetchRawPoints(fromInterval, untilInterval timestamp.Second, retentionID int) ([]Point, error) {
	r := &w.Retentions[retentionID]
	baseInterval, err := w.baseInterval(retentionID)
	if err != nil {
		return nil, err
	}
	log.Printf("fetchRawPoints, baseInterval=%s, fromInterval=%s, untilInterval=%s",
		formatTime(secondsToTime(int64(baseInterval))),
		formatTime(secondsToTime(int64(fromInterval))),
		formatTime(secondsToTime(int64(untilInterval))))

	fromOffset := r.pointOffsetAt(r.pointIndex(baseInterval, fromInterval))
	untilOffset := r.pointOffsetAt(r.pointIndex(baseInterval, untilInterval))
	log.Printf("fetchRawPoints, fromOffset=%d, untilOffset=%d",
		fromOffset, untilOffset)

	if fromOffset < untilOffset {
		fromPg := pageID(fromOffset / w.pageSize)
		untilPg := pageID(untilOffset / w.pageSize)
		if err := w.readPagesIfNeeded(fromPg, untilPg); err != nil {
			return nil, fmt.Errorf("cannot read page from %d to %d for points: %s",
				fromPg, untilPg, err)
		}
		points := make([]Point, (untilOffset-fromOffset)/pointSize)
		offset := fromOffset
		for i := range points {
			points[i].Time = w.timestampAt(offset)
			points[i].Value = w.float64At(offset + uint32Size)
			offset += pointSize
		}
		return points, nil
	}

	step := timestamp.Second(r.SecondsPerPoint)
	points := make([]Point, (untilInterval-fromInterval)/step)

	retentionStartOffset := int64(r.Offset)
	retentionEndOffset := retentionStartOffset + int64(r.NumberOfPoints)*pointSize
	log.Printf("fetchRawPoints, retentionStartOffset=%d, retentionEndOffset=%d, numberOfPoints=%d",
		retentionStartOffset, retentionEndOffset, r.NumberOfPoints)

	fromPg := pageID(fromOffset / w.pageSize)
	retentinoEndPg := pageID(retentionEndOffset / w.pageSize)
	if err := w.readPagesIfNeeded(fromPg, retentinoEndPg); err != nil {
		return nil, fmt.Errorf("cannot read page from %d to %d for points: %s",
			fromPg, retentinoEndPg, err)
	}

	retentinoStartPg := pageID(retentionStartOffset / w.pageSize)
	untilPg := pageID(untilOffset / w.pageSize)
	if err := w.readPagesIfNeeded(retentinoStartPg, untilPg); err != nil {
		return nil, fmt.Errorf("cannot read page from %d to %d for points: %s",
			retentinoStartPg, untilPg, err)
	}

	offset := fromOffset
	i := 0
	for offset < retentionEndOffset {
		points[i].Time = w.timestampAt(offset)
		points[i].Value = w.float64At(offset + uint32Size)
		offset += pointSize
		i++
	}
	offset = retentionStartOffset
	for offset < untilOffset {
		points[i].Time = w.timestampAt(offset)
		points[i].Value = w.float64At(offset + uint32Size)
		offset += pointSize
		i++
	}
	return points, nil
}

func clearOldPoints(points []Point, fromInterval, step timestamp.Second) {
	currentInterval := fromInterval
	for i := range points {
		if points[i].Time != currentInterval {
			points[i].Time = currentInterval
			points[i].Value = math.NaN()
		}
		currentInterval += step
	}
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
	v := binary.BigEndian.Uint32(buf)
	//log.Printf("uint32At offset=%d, buf=%v, v=%d, 0x%08x", offset, buf, v, v)
	return v
}

func (w *Whisper) uint64At(offset int64) uint64 {
	buf := w.bufForValue(offset, uint64Size)
	v := binary.BigEndian.Uint64(buf)
	//log.Printf("uint64At offset=%d, buf=%v, v=%d, 0x%016x", offset, buf, v, v)
	return v
}

func (w *Whisper) float32At(offset int64) float32 {
	return math.Float32frombits(w.uint32At(offset))
}

func (w *Whisper) float64At(offset int64) float64 {
	v := math.Float64frombits(w.uint64At(offset))
	//log.Printf("float64At offset=%d, v=%s", offset, strconv.FormatFloat(v, 'f', -1, 64))
	return v
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
	copy(buf[size-overflow:], p1[:overflow])
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
	t := timestamp.Second(w.uint32At(offset))
	//log.Printf("timestampAt offset=%d, t=%s", offset, formatTime(secondsToTime(int64(t))))
	return t
}

func (r *Retention) pointIndex(baseInterval, interval timestamp.Second) int {
	pointDistance := int64(interval-baseInterval) / int64(r.SecondsPerPoint)
	return int(floorMod(pointDistance, int64(r.NumberOfPoints)))
}

func (r *Retention) MaxRetention() uint64 {
	return uint64(r.SecondsPerPoint) * uint64(r.NumberOfPoints)
}

func (r *Retention) pointOffsetAt(index int) int64 {
	return int64(r.Offset) + int64(index)*pointSize
}

func (r *Retention) Interval(t timestamp.Second) timestamp.Second {
	step := int64(r.SecondsPerPoint)
	return timestamp.Second(int64(t) - floorMod(int64(t), step) + step)
}
