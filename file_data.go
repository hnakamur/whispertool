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
	"strings"
	"time"

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
type PointsList [][]Point

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

func NewFileData(m Meta, retentions []Retention) (*FileData, error) {
	if err := validateMetaAndRetentions(m, retentions); err != nil {
		return nil, err
	}
	d := &FileData{
		Meta:       m,
		Retentions: retentions,
	}
	d.fillDerivedValuesInHeader()
	d.buf = make([]byte, d.fileSizeFromHeader())
	d.setPagesDirtyByOffsetRange(0, uint32(len(d.buf)))
	d.putMeta()
	d.putRetentions()
	return d, nil
}

func NewFileDataRead(data []byte) (*FileData, error) {
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

	d.Meta.AggregationMethod = AggregationMethod(d.uint32At(0))
	d.Meta.maxRetention = Duration(d.uint32At(uint32Size))
	d.Meta.XFilesFactor = d.float32At(2 * uint32Size)

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
	} else if len(d.buf) > expectedSize {
		d.buf = d.buf[:expectedSize]
	}

	if d.Meta.maxRetention != d.Retentions[len(d.Retentions)-1].MaxRetention() {
		return errors.New("maxRetention in meta unmatch to maxRetention of lowest retention")
	}

	if err := validateMetaAndRetentions(d.Meta, d.Retentions); err != nil {
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

func (d *FileData) initDirtyPageBitSet() {
	pageCount := (len(d.buf) + pageSize - 1) / pageSize
	d.dirtyPageBitSet = bitset.New(uint(pageCount))
}

func (d *FileData) setPageDirty(page int) {
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

func (d *FileData) FlushTo(w io.WriterAt) error {
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

// FetchFromArchive fetches points from archive specified with `retentionID`.
// It fetches points in range between `from` (exclusive) and `until` (inclusive).
// If `now` is zero, the current time is used.
func (d *FileData) FetchFromArchive(retentionID int, from, until, now Timestamp) ([]Point, error) {
	if now == 0 {
		now = TimestampFromStdTime(time.Now())
	}
	//log.Printf("FetchFromArchive start, from=%s, until=%s, now=%s", from, until, now)
	if from > until {
		return nil, fmt.Errorf("invalid time interval: from time '%d' is after until time '%d'", from, until)
	}
	if retentionID < 0 || len(d.Retentions)-1 < retentionID {
		return nil, ErrRetentionIDOutOfRange
	}
	r := &d.Retentions[retentionID]

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

	fromInterval := r.interval(from)
	untilInterval := r.interval(until)
	step := r.SecondsPerPoint

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

func (d *FileData) PrintHeader(w io.Writer) error {
	m := &d.Meta
	_, err := fmt.Fprintf(w, "aggMethod:%s\taggMethodNum:%d\tmaxRetention:%s\txFileFactor:%s\tretentionCount:%d\n",
		m.AggregationMethod,
		int(m.AggregationMethod),
		m.maxRetention,
		strconv.FormatFloat(float64(m.XFilesFactor), 'f', -1, 32),
		m.retentionCount)
	if err != nil {
		return err
	}

	for i := range d.Retentions {
		r := &d.Retentions[i]
		_, err := fmt.Fprintf(w, "retentionDef:%d\tstep:%s\tnumberOfPoints:%d\toffset:%d\n",
			i,
			Duration(r.SecondsPerPoint),
			r.NumberOfPoints,
			r.offset)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pp PointsList) Print(w io.Writer) error {
	for i, points := range pp {
		for _, p := range points {
			_, err := fmt.Fprintf(w, "retId:%d\tt:%s\tval:%s\n", i, p.Time, p.Value)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *FileData) fillDerivedValuesInHeader() {
	d.Meta.maxRetention = d.Retentions[len(d.Retentions)-1].MaxRetention()
	d.Meta.retentionCount = uint32(len(d.Retentions))
	off := metaSize + len(d.Retentions)*retentionSize
	for i := range d.Retentions {
		r := &d.Retentions[i]
		r.offset = uint32(off)
		off += int(r.NumberOfPoints) * pointSize
	}
}

func (d *FileData) fileSizeFromHeader() int64 {
	sz := int64(metaSize)
	for i := range d.Retentions {
		r := &d.Retentions[i]
		sz += retentionSize + int64(r.NumberOfPoints)*pointSize
	}
	return sz
}

func validateMetaAndRetentions(m Meta, retentions []Retention) error {
	if err := m.validate(); err != nil {
		return err
	}
	if err := Retentions(retentions).validate(); err != nil {
		return err
	}
	return nil
}

func (m Meta) validate() error {
	if err := validateXFilesFactor(m.XFilesFactor); err != nil {
		return err
	}
	if err := validateAggregationMethod(m.AggregationMethod); err != nil {
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

func (d *FileData) putMeta() {
	d.putUint32At(uint32(d.Meta.AggregationMethod), 0)
	d.putUint32At(uint32(d.Meta.maxRetention), uint32Size)
	d.putFloat32At(d.Meta.XFilesFactor, 2*uint32Size)
	d.putUint32At(uint32(d.Meta.retentionCount), 3*uint32Size)
}

func (d *FileData) putRetentions() {
	off := uint32(metaSize)
	for i := range d.Retentions {
		r := &d.Retentions[i]
		d.putUint32At(r.offset, off)
		d.putUint32At(uint32(r.SecondsPerPoint), off+uint32Size)
		d.putUint32At(r.NumberOfPoints, off+2*uint32Size)
		off += retentionSize
	}
}

func (d *FileData) UpdatePointForArchive(retentionID int, t Timestamp, v Value, now Timestamp) error {
	points := []Point{{Time: t, Value: v}}
	return d.UpdatePointsForArchive(retentionID, points, now)
}

func (d *FileData) UpdatePointsForArchive(retentionID int, points []Point, now Timestamp) error {
	if now == 0 {
		now = TimestampFromStdTime(time.Now())
	}

	r := &d.Retentions[retentionID]
	points = r.filterPoints(points, now)
	if len(points) == 0 {
		return nil
	}

	sort.Stable(Points(points))
	alignedPoints := alignPoints(r, points)

	baseInterval := d.baseInterval(r)
	if baseInterval == 0 {
		baseInterval = r.intervalForWrite(now)
	}

	for _, p := range alignedPoints {
		offset := r.pointOffsetAt(r.pointIndex(baseInterval, p.Time))
		d.putPointAt(p, offset)
	}

	lowRetID := retentionID + 1
	if lowRetID < len(d.Retentions) {
		rLow := &d.Retentions[lowRetID]
		ts := rLow.timesToPropagate(alignedPoints)
		for ; lowRetID < len(d.Retentions) && len(ts) > 0; lowRetID++ {
			var err error
			ts, err = d.propagate(lowRetID, ts, now)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *Retention) timesToPropagate(points []Point) []Timestamp {
	var ts []Timestamp
	for _, p := range points {
		t := r.intervalForWrite(p.Time)
		if len(ts) > 0 && t == ts[len(ts)-1] {
			continue
		}
		ts = append(ts, t)
	}
	return ts
}

func (d *FileData) propagate(retentionID int, ts []Timestamp, now Timestamp) (propagatedTs []Timestamp, err error) {
	if len(ts) == 0 {
		return nil, nil
	}

	r := &d.Retentions[retentionID]
	baseInterval := d.baseInterval(r)
	if baseInterval == 0 {
		baseInterval = r.intervalForWrite(now)
	}

	step := r.SecondsPerPoint
	highRetID := retentionID - 1
	var rLow *Retention
	if retentionID+1 < len(d.Retentions) {
		rLow = &d.Retentions[retentionID+1]
	}

	for _, t := range ts {
		fromInterval := t
		untilInterval := t.Add(step)
		points := d.fetchRawPoints(highRetID, fromInterval, untilInterval)
		values := filterValidValues(points, fromInterval, step)
		knownFactor := float32(len(values)) / float32(len(points))
		if knownFactor < d.Meta.XFilesFactor {
			continue
		}

		v := aggregate(d.Meta.AggregationMethod, values)
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

func ParseRetentions(s string) ([]Retention, error) {
	if len(s) == 0 {
		return nil, fmt.Errorf("invalid retentions: %q", s)
	}
	var rr []Retention
	for {
		var rStr string
		i := strings.IndexRune(s, ',')
		if i == -1 {
			rStr = s
		} else {
			rStr = s[:i]
		}
		r, err := ParseRetention(rStr)
		if err != nil {
			return nil, fmt.Errorf("invalid retentions: %q", s)
		}
		rr = append(rr, *r)

		if i == -1 {
			break
		}
		if i+1 >= len(s) {
			return nil, fmt.Errorf("invalid retentions: %q", s)
		}
		s = s[i+1:]
	}
	return rr, nil
}

func ParseRetention(s string) (*Retention, error) {
	i := strings.IndexRune(s, ':')
	if i == -1 || i+1 >= len(s) {
		return nil, fmt.Errorf("invalid retention: %q", s)
	}

	step, err := ParseDuration(s[:i])
	if err != nil {
		return nil, fmt.Errorf("invalid retention: %q", s)
	}
	d, err := ParseDuration(s[i+1:])
	if err != nil {
		return nil, fmt.Errorf("invalid retention: %q", s)
	}
	if step <= 0 || d <= 0 || d%step != 0 {
		return nil, fmt.Errorf("invalid retention: %q", s)
	}
	return &Retention{
		SecondsPerPoint: step,
		NumberOfPoints:  uint32(d / step),
	}, nil
}

func (rr Retentions) String() string {
	var b strings.Builder
	for i, r := range rr {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(r.String())
	}
	return b.String()
}

func (rr Retentions) validate() error {
	if len(rr) == 0 {
		return fmt.Errorf("no retentions")
	}
	for i, r := range rr {
		if err := r.validate(); err != nil {
			return fmt.Errorf("invalid archive%v: %v", i, err)
		}

		if i == len(rr)-1 {
			break
		}

		rNext := rr[i+1]
		if !(r.SecondsPerPoint < rNext.SecondsPerPoint) {
			return fmt.Errorf("a Whisper database may not be configured having two archives with the same precision (archive%v: %v, archive%v: %v)", i, r, i+1, rNext)
		}

		if rNext.SecondsPerPoint%r.SecondsPerPoint != 0 {
			return fmt.Errorf("higher precision archives' precision must evenly divide all lower precision archives' precision (archive%v: %v, archive%v: %v)", i, r.SecondsPerPoint, i+1, rNext.SecondsPerPoint)
		}

		if r.MaxRetention() >= rNext.MaxRetention() {
			return fmt.Errorf("lower precision archives must cover larger time intervals than higher precision archives (archive%v: %v seconds, archive%v: %v seconds)", i, r.MaxRetention(), i+1, rNext.MaxRetention())
		}

		if r.NumberOfPoints < uint32(rNext.SecondsPerPoint/r.SecondsPerPoint) {
			return fmt.Errorf("each archive must have at least enough points to consolidate to the next archive (archive%v consolidates %v of archive%v's points but it has only %v total points)", i+1, rNext.SecondsPerPoint/r.SecondsPerPoint, i, r.NumberOfPoints)
		}
	}
	return nil
}

func (r Retention) validate() error {
	if r.SecondsPerPoint <= 0 {
		return errors.New("seconds per point must be positive")
	}
	if r.NumberOfPoints <= 0 {
		return errors.New("number of points must be positive")
	}
	return nil
}

func (r Retention) String() string {
	return r.SecondsPerPoint.String() + ":" +
		(r.SecondsPerPoint * Duration(r.NumberOfPoints)).String()
}

func (r *Retention) pointIndex(baseInterval, interval Timestamp) int {
	// NOTE: We use interval.Sub(baseInterval) here instead of
	// interval - baseInterval since the latter produces
	// wrong values because of underflow when interval < baseInterval.
	// Another solution would be (int64(interval) - int64(baseInterval))
	pointDistance := int64(interval.Sub(baseInterval)) / int64(r.SecondsPerPoint)
	return int(floorMod(pointDistance, int64(r.NumberOfPoints)))
}

func (r *Retention) MaxRetention() Duration {
	return r.SecondsPerPoint * Duration(r.NumberOfPoints)
}

func (r *Retention) pointOffsetAt(index int) uint32 {
	return r.offset + uint32(index)*pointSize
}

func (r *Retention) interval(t Timestamp) Timestamp {
	step := int64(r.SecondsPerPoint)
	return Timestamp(int64(t) - floorMod(int64(t), step) + step)
}

func (r *Retention) intervalForWrite(t Timestamp) Timestamp {
	step := int64(r.SecondsPerPoint)
	return Timestamp(int64(t) - floorMod(int64(t), step))
}

func (r *Retention) filterPoints(points []Point, now Timestamp) []Point {
	oldest := r.intervalForWrite(now.Add(-r.MaxRetention()))
	filteredPoints := make([]Point, 0, len(points))
	for _, p := range points {
		if p.Time >= oldest && p.Time <= now {
			filteredPoints = append(filteredPoints, p)
		}
	}
	return filteredPoints
}

// points must be sorted by Time
func alignPoints(r *Retention, points []Point) []Point {
	alignedPoints := make([]Point, 0, len(points))
	var prevTime Timestamp
	for i, point := range points {
		dPoint := Point{
			Time:  r.intervalForWrite(point.Time),
			Value: point.Value,
		}
		if i > 0 && point.Time == prevTime {
			alignedPoints[len(alignedPoints)-1].Value = dPoint.Value
		} else {
			alignedPoints = append(alignedPoints, dPoint)
			prevTime = dPoint.Time
		}
	}
	return alignedPoints
}

func (v *Value) SetNaN() {
	*v = Value(math.NaN())
}

func (v Value) IsNaN() bool {
	return math.IsNaN(float64(v))
}

func (v Value) String() string {
	return strconv.FormatFloat(float64(v), 'f', -1, 64)
}

func (pl PointsList) Diff(ql [][]Point) ([][]Point, [][]Point) {
	if len(pl) != len(ql) {
		return pl, ql
	}

	pl2 := make([][]Point, len(pl))
	ql2 := make([][]Point, len(ql))
	for i, pp := range pl {
		pl2[i], ql2[i] = Points(pp).Diff(ql[i])
	}
	return pl2, ql2
}

func (pp Points) Diff(qq []Point) ([]Point, []Point) {
	if len(pp) != len(qq) {
		return pp, qq
	}

	var pp2, qq2 []Point
	for i, p := range pp {
		q := qq[i]
		if !p.Equal(q) {
			pp2 = append(pp2, p)
			qq2 = append(qq2, q)
		}
	}
	return pp2, qq2
}

func (p Point) Equal(q Point) bool {
	if p.Time != q.Time {
		return false
	}
	pIsNaN := p.Value.IsNaN()
	qIsNaN := q.Value.IsNaN()
	return (pIsNaN && qIsNaN) ||
		(!pIsNaN && !qIsNaN && p.Value == q.Value)
}

func (pp Points) Len() int           { return len(pp) }
func (pp Points) Less(i, j int) bool { return pp[i].Time < pp[j].Time }
func (pp Points) Swap(i, j int)      { pp[i], pp[j] = pp[j], pp[i] }
