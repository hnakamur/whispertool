package whispertool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/willf/bitset"
)

type fileData struct {
	meta       meta
	retentions []Retention

	buf             []byte
	dirtyPageBitSet *bitset.BitSet
}

type meta struct {
	aggregationMethod AggregationMethod
	maxRetention      Duration
	xFilesFactor      float32
	retentionCount    uint32
}

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
var ErrRetentionIDOutOfRange = errors.New("retention ID out of range")

func newFileData(retentions []Retention, aggregationMethod AggregationMethod, xFilesFactor float32) (*fileData, error) {
	m := meta{
		aggregationMethod: aggregationMethod,
		xFilesFactor:      xFilesFactor,
	}
	if err := validateMetaAndRetentions(m, retentions); err != nil {
		return nil, err
	}
	d := &fileData{
		meta:       m,
		retentions: retentions,
	}
	d.fillDerivedValuesInHeader()
	d.buf = make([]byte, d.fileSizeFromHeader())
	d.setPagesDirtyByOffsetRange(0, uint32(len(d.buf)))
	d.putMeta()
	d.putRetentions()
	return d, nil
}

func newFileDataRead(data []byte) (*fileData, error) {
	d := &fileData{buf: data}
	if err := d.readMetaAndRetentions(); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *fileData) AggregationMethod() AggregationMethod { return d.meta.aggregationMethod }
func (d *fileData) XFilesFactor() float32                { return d.meta.xFilesFactor }
func (d *fileData) Retentions() []Retention              { return d.retentions }

func (d *fileData) readMetaAndRetentions() error {
	expectedSize := metaSize
	if len(d.buf) < expectedSize {
		return io.ErrUnexpectedEOF
	}

	d.meta.aggregationMethod = AggregationMethod(d.uint32At(0))
	d.meta.maxRetention = Duration(d.uint32At(uint32Size))
	d.meta.xFilesFactor = d.float32At(2 * uint32Size)

	d.meta.retentionCount = d.uint32At(3 * uint32Size)
	if d.meta.retentionCount == 0 {
		return errors.New("retention count must not be zero")
	}

	expectedSize += int(d.meta.retentionCount) * retentionSize
	if len(d.buf) < expectedSize {
		return io.ErrUnexpectedEOF
	}

	d.retentions = make([]Retention, d.meta.retentionCount)
	off := uint32(metaSize)
	for i := 0; i < int(d.meta.retentionCount); i++ {
		r := &d.retentions[i]
		r.offset = d.uint32At(off)
		if int(r.offset) != expectedSize {
			return fmt.Errorf("unexpected offset for retention %d, got: %d, want: %d", i, r.offset, expectedSize)
		}
		r.secondsPerPoint = Duration(d.uint32At(off + uint32Size))
		r.numberOfPoints = d.uint32At(off + 2*uint32Size)

		off += retentionSize
		expectedSize += int(r.numberOfPoints) * pointSize
	}

	if len(d.buf) < expectedSize {
		return io.ErrUnexpectedEOF
	} else if len(d.buf) > expectedSize {
		d.buf = d.buf[:expectedSize]
	}

	if d.meta.maxRetention != d.retentions[len(d.retentions)-1].MaxRetention() {
		return errors.New("maxRetention in meta unmatch to maxRetention of lowest retention")
	}

	if err := validateMetaAndRetentions(d.meta, d.retentions); err != nil {
		return err
	}

	return nil
}

func (d *fileData) baseInterval(r *Retention) Timestamp {
	return d.timestampAt(r.offset)
}

func (d *fileData) pointAt(offset uint32) Point {
	return Point{
		Time:  d.timestampAt(offset),
		Value: d.valueAt(offset + uint32Size),
	}
}

func (d *fileData) timestampAt(offset uint32) Timestamp {
	return Timestamp(d.uint32At(offset))
}

func (d *fileData) valueAt(offset uint32) Value {
	return Value(d.float64At(offset))
}

func (d *fileData) float32At(offset uint32) float32 {
	return math.Float32frombits(d.uint32At(offset))
}

func (d *fileData) float64At(offset uint32) float64 {
	return math.Float64frombits(d.uint64At(offset))
}

func (d *fileData) uint32At(offset uint32) uint32 {
	return binary.BigEndian.Uint32(d.buf[offset:])
}

func (d *fileData) uint64At(offset uint32) uint64 {
	return binary.BigEndian.Uint64(d.buf[offset:])
}

func (d *fileData) putPointAt(p Point, offset uint32) {
	d.putTimestampAt(p.Time, offset)
	d.putValueAt(p.Value, offset+uint32Size)
}

func (d *fileData) putTimestampAt(t Timestamp, offset uint32) {
	d.putUint32At(uint32(t), offset)
}

func (d *fileData) putValueAt(v Value, offset uint32) {
	d.putFloat64At(float64(v), offset)
}

func (d *fileData) putFloat32At(v float32, offset uint32) {
	d.putUint32At(math.Float32bits(v), offset)
}

func (d *fileData) putFloat64At(v float64, offset uint32) {
	d.putUint64At(math.Float64bits(v), offset)
}

func (d *fileData) putUint32At(v uint32, offset uint32) {
	d.setPagesDirtyByOffsetRange(offset, uint32Size)
	binary.BigEndian.PutUint32(d.buf[offset:], v)
}

func (d *fileData) putUint64At(v uint64, offset uint32) {
	d.setPagesDirtyByOffsetRange(offset, uint64Size)
	binary.BigEndian.PutUint64(d.buf[offset:], v)
}

func (d *fileData) setPagesDirtyByOffsetRange(offset, size uint32) {
	startPage := pageForStartOffset(offset)
	endPage := pageForEndOffset(offset + size)
	for page := startPage; page <= endPage; page++ {
		d.setPageDirty(page)
	}
}

func (d *fileData) initDirtyPageBitSet() {
	pageCount := (len(d.buf) + pageSize - 1) / pageSize
	d.dirtyPageBitSet = bitset.New(uint(pageCount))
}

func (d *fileData) setPageDirty(page int) {
	if d.dirtyPageBitSet == nil {
		d.initDirtyPageBitSet()
	}
	d.dirtyPageBitSet.Set(uint(page))
}

func pageForStartOffset(offset uint32) int {
	return int(offset) / pageSize
}

func pageForEndOffset(offset uint32) int {
	return int(offset-1) / pageSize
}

func (d *fileData) FlushTo(w io.WriterAt) error {
	for _, r := range dirtyPageRanges(d.dirtyPageBitSet) {
		off := r.start * pageSize
		end := r.end * pageSize
		if end > len(d.buf) {
			end = len(d.buf)
		}
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

func (d *fileData) GetAllRawUnsortedPoints(retentionID int) []Point {
	r := &d.retentions[retentionID]
	points := make([]Point, r.numberOfPoints)
	off := r.offset
	for i := 0; i < len(points); i++ {
		points[i] = d.pointAt(off)
		off += pointSize
	}
	return points
}

func (d *fileData) fetchRawPoints(retentionID int, fromInterval, untilInterval Timestamp) []Point {
	r := &d.retentions[retentionID]
	baseInterval := d.baseInterval(r)

	step := r.secondsPerPoint
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
	arcEndOffset := arcStartOffset + r.numberOfPoints*pointSize

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

// FetchFromArchive fetches points from archive specified with `retentionID`.
// It fetches points in range between `from` (exclusive) and `until` (inclusive).
// If `now` is zero, the current time is used.
func (d *fileData) FetchFromArchive(retentionID int, from, until, now Timestamp) ([]Point, error) {
	if now == 0 {
		now = TimestampFromStdTime(time.Now())
	}
	//log.Printf("FetchFromArchive start, from=%s, until=%s, now=%s", from, until, now)
	if from > until {
		return nil, fmt.Errorf("invalid time interval: from time '%d' is after until time '%d'", from, until)
	}
	if retentionID < 0 || len(d.retentions)-1 < retentionID {
		return nil, ErrRetentionIDOutOfRange
	}
	r := &d.retentions[retentionID]

	oldest := now.Add(-r.MaxRetention())
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
	//log.Printf("FetchFromArchive adjusted, from=%s, until=%s, now=%s", from, until, now)

	baseInterval := d.baseInterval(r)
	//log.Printf("FetchFromArchive retentionID=%d, baseInterval=%s", retentionID, baseInterval)

	fromInterval := r.Interval(from)
	untilInterval := r.Interval(until)
	step := r.secondsPerPoint

	if baseInterval == 0 {
		points := make([]Point, (untilInterval-fromInterval)/Timestamp(step))
		t := fromInterval
		for i := range points {
			points[i].Time = t
			points[i].Value.SetNaN()
			t = t.Add(step)
		}
		return points, nil
	}

	// Zero-length time range: always include the next point
	if fromInterval == untilInterval {
		untilInterval = untilInterval.Add(step)
	}

	points := d.fetchRawPoints(retentionID, fromInterval, untilInterval)
	//log.Printf("FetchFromArchive after fetchRawPoints, retentionID=%d, len(points)=%d", retentionID, len(points))
	//for i, pt := range points {
	//	log.Printf("rawPoint i=%d, time=%s, value=%s", i, pt.Time, pt.Value)
	//}
	clearOldPoints(points, fromInterval, step)
	//log.Printf("FetchFromArchive after clearOldPoints, retentionID=%d, len(points)=%d", retentionID, len(points))

	return points, nil
}

func clearOldPoints(points []Point, fromInterval Timestamp, step Duration) {
	currentInterval := fromInterval
	for i := range points {
		if points[i].Time != currentInterval {
			points[i].Time = currentInterval
			points[i].Value.SetNaN()
		}
		currentInterval = currentInterval.Add(step)
	}
}

func (d *fileData) PrintHeader(w io.Writer) error {
	m := &d.meta
	_, err := fmt.Fprintf(w, "aggMethod:%s\taggMethodNum:%d\tmaxRetention:%s\txFileFactor:%s\tretentionCount:%d\n",
		m.aggregationMethod,
		int(m.aggregationMethod),
		m.maxRetention,
		strconv.FormatFloat(float64(m.xFilesFactor), 'f', -1, 32),
		m.retentionCount)
	if err != nil {
		return err
	}

	for i := range d.retentions {
		r := &d.retentions[i]
		_, err := fmt.Fprintf(w, "retentionDef:%d\tstep:%s\tnumberOfPoints:%d\toffset:%d\n",
			i,
			Duration(r.secondsPerPoint),
			r.numberOfPoints,
			r.offset)
		if err != nil {
			return err
		}
	}
	return nil
}

// Bytes returns data for whole file.
func (d *fileData) Bytes() []byte {
	return d.buf
}

func (d *fileData) fillDerivedValuesInHeader() {
	d.meta.maxRetention = d.retentions[len(d.retentions)-1].MaxRetention()
	d.meta.retentionCount = uint32(len(d.retentions))
	off := metaSize + len(d.retentions)*retentionSize
	for i := range d.retentions {
		r := &d.retentions[i]
		r.offset = uint32(off)
		off += int(r.numberOfPoints) * pointSize
	}
}

func (d *fileData) fileSizeFromHeader() int64 {
	sz := int64(metaSize)
	for i := range d.retentions {
		r := &d.retentions[i]
		sz += retentionSize + int64(r.numberOfPoints)*pointSize
	}
	return sz
}

func validateMetaAndRetentions(m meta, retentions []Retention) error {
	if err := m.validate(); err != nil {
		return err
	}
	if err := Retentions(retentions).validate(); err != nil {
		return err
	}
	return nil
}

func (m meta) validate() error {
	if err := validateXFilesFactor(m.xFilesFactor); err != nil {
		return err
	}
	if err := validateAggregationMethod(m.aggregationMethod); err != nil {
		return err
	}
	return nil
}

func validateXFilesFactor(xFilesFactor float32) error {
	if xFilesFactor < 0 || 1 < xFilesFactor {
		return errors.New("invalid XFilesFactor")
	}
	return nil
}

func validateAggregationMethod(aggMethod AggregationMethod) error {
	switch aggMethod {
	case Average, Sum, Last, Max, Min, First:
		return nil
	default:
		return errors.New("invalid aggregation method")
	}
}

func (d *fileData) putMeta() {
	d.putUint32At(uint32(d.meta.aggregationMethod), 0)
	d.putUint32At(uint32(d.meta.maxRetention), uint32Size)
	d.putFloat32At(d.meta.xFilesFactor, 2*uint32Size)
	d.putUint32At(uint32(d.meta.retentionCount), 3*uint32Size)
}

func (d *fileData) putRetentions() {
	off := uint32(metaSize)
	for i := range d.retentions {
		r := &d.retentions[i]
		d.putUint32At(r.offset, off)
		d.putUint32At(uint32(r.secondsPerPoint), off+uint32Size)
		d.putUint32At(r.numberOfPoints, off+2*uint32Size)
		off += retentionSize
	}
}

func (d *fileData) UpdatePointForArchive(retentionID int, t Timestamp, v Value, now Timestamp) error {
	points := []Point{{Time: t, Value: v}}
	return d.UpdatePointsForArchive(retentionID, points, now)
}

func (d *fileData) UpdatePointsForArchive(retentionID int, points []Point, now Timestamp) error {
	if now == 0 {
		now = TimestampFromStdTime(time.Now())
	}

	r := &d.retentions[retentionID]
	points = r.filterPoints(points, now)
	if len(points) == 0 {
		return nil
	}

	sort.Stable(Points(points))
	alignedPoints := r.alignPoints(points)

	baseInterval := d.baseInterval(r)
	if baseInterval == 0 {
		baseInterval = r.intervalForWrite(now)
	}

	for _, p := range alignedPoints {
		offset := r.pointOffsetAt(r.pointIndex(baseInterval, p.Time))
		d.putPointAt(p, offset)
	}

	lowRetID := retentionID + 1
	if lowRetID < len(d.retentions) {
		rLow := &d.retentions[lowRetID]
		ts := rLow.timesToPropagate(alignedPoints)
		for ; lowRetID < len(d.retentions) && len(ts) > 0; lowRetID++ {
			var err error
			ts, err = d.propagate(lowRetID, ts, now)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *fileData) propagate(retentionID int, ts []Timestamp, now Timestamp) (propagatedTs []Timestamp, err error) {
	if len(ts) == 0 {
		return nil, nil
	}

	r := &d.retentions[retentionID]
	baseInterval := d.baseInterval(r)
	if baseInterval == 0 {
		baseInterval = r.intervalForWrite(now)
	}

	step := r.secondsPerPoint
	highRetID := retentionID - 1
	var rLow *Retention
	if retentionID+1 < len(d.retentions) {
		rLow = &d.retentions[retentionID+1]
	}

	for _, t := range ts {
		fromInterval := t
		untilInterval := t.Add(step)
		points := d.fetchRawPoints(highRetID, fromInterval, untilInterval)
		values := filterValidValues(points, fromInterval, step)
		knownFactor := float32(len(values)) / float32(len(points))
		if knownFactor < d.meta.xFilesFactor {
			continue
		}

		v := aggregate(d.meta.aggregationMethod, values)
		offset := r.pointOffsetAt(r.pointIndex(baseInterval, t))
		d.putTimestampAt(t, offset)
		d.putValueAt(v, offset+uint32Size)

		if rLow != nil {
			tLow := rLow.intervalForWrite(t)
			if len(propagatedTs) == 0 || propagatedTs[len(propagatedTs)-1] != tLow {
				propagatedTs = append(propagatedTs, tLow)
			}
		}
	}

	return propagatedTs, nil
}

func filterValidValues(points []Point, fromInterval Timestamp, step Duration) []Value {
	values := make([]Value, 0, len(points))
	currentInterval := fromInterval
	for _, p := range points {
		if p.Time != currentInterval {
			continue
		}
		values = append(values, p.Value)
		currentInterval = currentInterval.Add(step)
	}
	return values
}

func sum(values []Value) Value {
	result := Value(0)
	for _, value := range values {
		result += value
	}
	return result
}

func aggregate(method AggregationMethod, knownValues []Value) Value {
	switch method {
	case Average:
		return sum(knownValues) / Value(len(knownValues))
	case Sum:
		return sum(knownValues)
	case First:
		return knownValues[0]
	case Last:
		return knownValues[len(knownValues)-1]
	case Max:
		max := knownValues[0]
		for _, value := range knownValues {
			if value > max {
				max = value
			}
		}
		return max
	case Min:
		min := knownValues[0]
		for _, value := range knownValues {
			if value < min {
				min = value
			}
		}
		return min
	}
	panic("Invalid aggregation method")
}
