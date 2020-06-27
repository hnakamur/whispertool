package whispertool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/willf/bitset"
)

type FileData struct {
	Meta       Meta
	Retentions []Retention

	buf             []byte
	dirtyPageBitSet *bitset.BitSet
}

type Meta struct {
	AggregationMethod AggregationMethod
	maxRetention      Duration
	XFilesFactor      float32
	retentionCount    uint32
}

type Retention struct {
	offset          uint32
	SecondsPerPoint Duration
	NumberOfPoints  uint32
}

type Retentions []Retention

type Value float64

type Point struct {
	Time  Timestamp
	Value Value
}

type Points []Point

type pageRange struct {
	start int
	end   int
}

const (
	uint32Size    = 4
	uint64Size    = 8
	float32Size   = 4
	float64Size   = 8
	metaSize      = 3*uint32Size + float32Size
	retentionSize = 3 * uint32Size
	pointSize     = uint32Size + float64Size
)

var pageSize = os.Getpagesize()

func NewFileData(data []byte) (*FileData, error) {
	d := &FileData{buf: data}
	if err := d.readMetaAndRetentions(); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *FileData) readMetaAndRetentions() error {
	expectedSize := metaSize
	if len(d.buf) < expectedSize {
		return io.ErrUnexpectedEOF
	}

	aggMethod := AggregationMethod(d.uint32At(0))
	switch aggMethod {
	case Average, Sum, Last, Max, Min, First:
		d.Meta.AggregationMethod = aggMethod
	default:
		return errors.New("invalid aggregation method")
	}

	d.Meta.maxRetention = Duration(d.uint32At(uint32Size))

	d.Meta.XFilesFactor = d.float32At(2 * uint32Size)
	if d.Meta.XFilesFactor < 0 || 1 < d.Meta.XFilesFactor {
		return errors.New("invalid XFilesFactor")
	}

	d.Meta.retentionCount = d.uint32At(3 * uint32Size)
	if d.Meta.retentionCount == 0 {
		return errors.New("retention count must not be zero")
	}

	expectedSize += int(d.Meta.retentionCount) * retentionSize
	if len(d.buf) < expectedSize {
		return io.ErrUnexpectedEOF
	}

	d.Retentions = make([]Retention, d.Meta.retentionCount)
	off := uint32(metaSize)
	for i := 0; i < int(d.Meta.retentionCount); i++ {
		r := &d.Retentions[i]
		r.offset = d.uint32At(off)
		if int(r.offset) != expectedSize {
			return fmt.Errorf("unexpected offset for retention %d, got: %d, want: %d", i, r.offset, expectedSize)
		}
		r.SecondsPerPoint = Duration(d.uint32At(off + uint32Size))
		r.NumberOfPoints = d.uint32At(off + 2*uint32Size)

		off += retentionSize
		expectedSize += int(r.NumberOfPoints) * pointSize
	}

	if len(d.buf) < expectedSize {
		return io.ErrUnexpectedEOF
	}

	if d.Meta.maxRetention != d.Retentions[len(d.Retentions)-1].MaxRetention() {
		return errors.New("maxRetention in meta unmatch to maxRetention of lowest retention")
	}

	if err := Retentions(d.Retentions).validate(); err != nil {
		return err
	}

	return nil
}

func (d *FileData) baseInterval(r *Retention) Timestamp {
	return d.timestampAt(r.offset)
}

func (d *FileData) pointAt(offset uint32) Point {
	return Point{
		Time:  d.timestampAt(offset),
		Value: d.valueAt(offset + uint32Size),
	}
}

func (d *FileData) timestampAt(offset uint32) Timestamp {
	return Timestamp(d.uint32At(offset))
}

func (d *FileData) valueAt(offset uint32) Value {
	return Value(d.float64At(offset))
}

func (d *FileData) float32At(offset uint32) float32 {
	return math.Float32frombits(d.uint32At(offset))
}

func (d *FileData) float64At(offset uint32) float64 {
	return math.Float64frombits(d.uint64At(offset))
}

func (d *FileData) uint32At(offset uint32) uint32 {
	return binary.BigEndian.Uint32(d.buf[offset:])
}

func (d *FileData) uint64At(offset uint32) uint64 {
	return binary.BigEndian.Uint64(d.buf[offset:])
}

func (d *FileData) putPointAt(p Point, offset uint32) {
	d.putTimestampAt(p.Time, offset)
	d.putValueAt(p.Value, offset+uint32Size)
}

func (d *FileData) putTimestampAt(t Timestamp, offset uint32) {
	d.putUint32At(uint32(t), offset)
}

func (d *FileData) putValueAt(v Value, offset uint32) {
	d.putFloat64At(float64(v), offset)
}

func (d *FileData) putFloat32At(v float32, offset uint32) {
	d.putUint32At(math.Float32bits(v), offset)
}

func (d *FileData) putFloat64At(v float64, offset uint32) {
	d.putUint64At(math.Float64bits(v), offset)
}

func (d *FileData) putUint32At(v uint32, offset uint32) {
	d.setPagesDirtyByOffsetRange(offset, uint32Size)
	binary.BigEndian.PutUint32(d.buf[offset:], v)
}

func (d *FileData) putUint64At(v uint64, offset uint32) {
	d.setPagesDirtyByOffsetRange(offset, uint64Size)
	binary.BigEndian.PutUint64(d.buf[offset:], v)
}

func (d *FileData) setPagesDirtyByOffsetRange(offset, size uint32) {
	startPage := pageForStartOffset(offset)
	endPage := pageForEndOffset(offset + size)
	for page := startPage; page <= endPage; page++ {
		d.setPageDirty(page)
	}
}

func (d *FileData) setPageDirty(page int) {
	if d.dirtyPageBitSet == nil {
		pageCount := (len(d.buf) + pageSize - 1) / pageSize
		d.dirtyPageBitSet = bitset.New(uint(pageCount))
	}
	d.dirtyPageBitSet.Set(uint(page))
}

func pageForStartOffset(offset uint32) int {
	return int(offset) / pageSize
}

func pageForEndOffset(offset uint32) int {
	return int(offset-1) / pageSize
}

func (d *FileData) FlushTo(w io.WriterAt) error {
	for _, r := range dirtyPageRanges(d.dirtyPageBitSet) {
		off := r.start * pageSize
		end := r.end * pageSize
		if _, err := w.WriteAt(d.buf[off:end], int64(off)); err != nil {
			return err
		}
	}
	d.dirtyPageBitSet.ClearAll()
	return nil
}

func dirtyPageRanges(bitSet *bitset.BitSet) []pageRange {
	if bitSet == nil {
		return nil
	}

	var ranges []pageRange
	var i, count uint
	for ; i < bitSet.Len(); i++ {
		if bitSet.Test(i) {
			count++
			continue
		}

		if count > 0 {
			ranges = append(ranges, pageRange{
				start: int(i - count),
				end:   int(i),
			})
			count = 0
		}
	}
	if count > 0 {
		ranges = append(ranges, pageRange{
			start: int(i - count),
			end:   int(i),
		})
	}
	return ranges
}

func (d *FileData) getAllRawUnsortedPoints(retentionID int) []Point {
	r := &d.Retentions[retentionID]
	points := make([]Point, r.NumberOfPoints)
	off := r.offset
	for i := 0; i < len(points); i++ {
		points[i] = d.pointAt(off)
		off += pointSize
	}
	return points
}

func (d *FileData) fetchRawPoints(retentionID int, fromInterval, untilInterval Timestamp) []Point {
	r := &d.Retentions[retentionID]
	baseInterval := d.baseInterval(r)

	step := r.SecondsPerPoint
	points := make([]Point, untilInterval.Sub(fromInterval)/step)

	fromOffset := r.pointOffsetAt(r.pointIndex(baseInterval, fromInterval))
	untilOffset := r.pointOffsetAt(r.pointIndex(baseInterval, untilInterval))
	if fromOffset < untilOffset {
		i := 0
		for off := fromOffset; off < untilOffset; off += pointSize {
			points[i] = d.pointAt(off)
			i++
		}
		return points
	}

	arcStartOffset := r.offset
	arcEndOffset := arcStartOffset + r.NumberOfPoints*pointSize

	i := 0
	for off := fromOffset; off < arcEndOffset; off += pointSize {
		points[i] = d.pointAt(off)
		i++
	}
	for off := arcStartOffset; off < untilOffset; off += pointSize {
		points[i] = d.pointAt(off)
		i++
	}
	return points
}
