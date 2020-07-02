package whispertool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"syscall"
	"time"

	"github.com/willf/bitset"
)

// Whisper represents a Whisper database file.
type Whisper struct {
	meta       meta
	retentions []Retention

	buf             []byte
	dirtyPageBitSet *bitset.BitSet

	file *os.File

	openFileFlag int
	flock        bool
	perm         os.FileMode
	inMemory     bool
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

// Option is the type for options for creating or opening a whisper file.
type Option func(*Whisper)

// WithInMemory enables to create a whisper database in memory without creating a file.
// This options is useful for only for `Create`.
func WithInMemory() Option {
	return func(w *Whisper) {
		w.inMemory = true
	}
}

// WithFlock enables flock for the file.
// This option is useful only when no WithInMemory is passed.
func WithFlock() Option {
	return func(w *Whisper) {
		w.flock = true
	}
}

// WithRawData set the raw data for the whisper file.
// If this option is used, retentions, aggregationMethod, and xFilesFactor arguments
// passed to Create will be ignored.
// This options is useful for only for Create.
func WithRawData(data []byte) Option {
	return func(w *Whisper) {
		w.buf = data
	}
}

// WithOpenFileFlag sets the flag for opening the file.
// This option is useful only when no WithInMemory is passed.
// Without this option, the default value is
// os.O_RDWR | os.O_CREATE | os.O_EXCL for Create and
// os.O_RDWR for Open.
func WithOpenFileFlag(flag int) Option {
	return func(w *Whisper) {
		w.openFileFlag = flag
	}
}

// WithPerm sets the permission for the file.
// This option is useful only when no WithInMemory is passed.
// Without this option, the default value is 0644.
func WithPerm(perm os.FileMode) Option {
	return func(w *Whisper) {
		w.perm = perm
	}
}

// Create creates a whisper database file.
func Create(filename string, retentions []Retention, aggregationMethod AggregationMethod, xFilesFactor float32, opts ...Option) (*Whisper, error) {
	w := &Whisper{
		openFileFlag: os.O_RDWR | os.O_CREATE | os.O_EXCL,
		perm:         0644,
	}
	for _, opt := range opts {
		opt(w)
	}

	if !w.inMemory {
		err := w.openAndLockFile(filename)
		if err != nil {
			return nil, err
		}
	}

	if w.buf != nil {
		if err := w.readMetaAndRetentions(); err != nil {
			return nil, err
		}
		w.setPagesDirtyByOffsetRange(0, uint32(len(w.buf)))
	} else {
		sort.Sort(retentionsByPrecision(retentions))
		if err := w.initNewBuf(retentions, aggregationMethod, xFilesFactor); err != nil {
			return nil, err
		}
	}
	return w, nil
}

// Open opens an existing whisper database file.
// In the current implementation, the whole content is read after opening the file.
// This behavior may be changed in the future.
func Open(filename string, opts ...Option) (*Whisper, error) {
	w := &Whisper{
		openFileFlag: os.O_RDWR,
		perm:         0644,
	}
	for _, opt := range opts {
		opt(w)
	}

	err := w.openAndLockFile(filename)
	if err != nil {
		return nil, err
	}

	st, err := w.file.Stat()
	if err != nil {
		return nil, err
	}

	buf := make([]byte, st.Size())
	if _, err := io.ReadFull(w.file, buf); err != nil {
		return nil, err
	}
	w.buf = buf

	if err := w.readMetaAndRetentions(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *Whisper) openAndLockFile(filename string) error {
	file, err := os.OpenFile(filename, w.openFileFlag, w.perm)
	if err != nil {
		return err
	}
	w.file = file

	if w.flock {
		if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
			file.Close()
			return err
		}
	}
	return nil
}

// Sync flushes modifications on the memory buffer to the file and
// sync commits the content to the storage by calling os.File.Sync().
// Note it is caller's responsibility to call Sync and the modification
// will be lost without calling Sync.
// For the file created with WithInMemory, this is a no-op.
func (w *Whisper) Sync() error {
	if w.inMemory {
		return nil
	}

	if err := w.flushTo(w.file); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	return nil
}

// Close closes the file.
// For the file created with WithInMemory, this is a no-op.
func (w *Whisper) Close() error {
	if w.inMemory {
		return nil
	}

	return w.file.Close()
}

// AggregationMethod returns the aggregation method of the whisper file.
func (w *Whisper) AggregationMethod() AggregationMethod { return w.meta.aggregationMethod }

// XFilesFactor returns the xFilesFactor of the whisper file.
func (w *Whisper) XFilesFactor() float32 { return w.meta.xFilesFactor }

// MaxRetention returns the max retention of the whisper file.
func (w *Whisper) MaxRetention() Duration { return w.meta.maxRetention }

// Retentions returns the retentions of the whisper file.
func (w *Whisper) Retentions() Retentions { return w.retentions }

// RawData returns data for whole file.
// Note the byte slice returned is the internal work buffer,
// without cloning in favor of performance.
// It is caller's responsibility to not modify the data.
func (w *Whisper) RawData() []byte {
	return w.buf
}

// Fetch fetches points from the best archive for the specified time range.
//
// It fetches points in range between `from` (exclusive) and `until` (inclusive).
func (w *Whisper) Fetch(from, until Timestamp) (*TimeSeries, error) {
	return w.FetchFromArchive(RetentionIDBest, from, until, 0)
}

// FetchFromArchive fetches points in the specified archive and the time range.
//
// FetchFromArchive fetches points from archive specified with `retentionID`.
// It fetches points in range between `from` (exclusive) and `until` (inclusive).
// If `now` is zero, the current time is used.
func (w *Whisper) FetchFromArchive(retentionID int, from, until, now Timestamp) (*TimeSeries, error) {
	if now == 0 {
		now = TimestampFromStdTime(time.Now())
	}
	log.Printf("FetchFromArchive start, from=%s, until=%s, now=%s", from, until, now)
	if from > until {
		return nil, fmt.Errorf("invalid time interval: from time '%d' is after until time '%d'", from, until)
	}
	if (retentionID != RetentionIDBest && retentionID < 0) || len(w.retentions)-1 < retentionID {
		return nil, ErrRetentionIDOutOfRange
	}
	if retentionID == RetentionIDBest {
		retentionID = w.findBestRetention(from, now)
		log.Printf("found best retentionID=%d", retentionID)
	}
	r := &w.retentions[retentionID]

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
	log.Printf("FetchFromArchive adjusted, from=%s, until=%s, now=%s", from, until, now)

	baseInterval := w.baseInterval(r)
	log.Printf("FetchFromArchive retentionID=%d, retention=%s, baseInterval=%s", retentionID, r, baseInterval)
	log.Printf("FetchFromArchive retentions=%s", w.Retentions())

	fromInterval := r.interval(from)
	untilInterval := r.interval(until)
	step := r.secondsPerPoint

	if baseInterval == 0 {
		points := make([]Point, (untilInterval-fromInterval)/Timestamp(step))
		t := fromInterval
		for i := range points {
			points[i].Time = t
			points[i].Value.SetNaN()
			t = t.Add(step)
		}
		return &TimeSeries{
			fromTime:  fromInterval,
			untilTime: untilInterval,
			step:      step,
			points:    points,
		}, nil
	}

	// Zero-length time range: always include the next point
	if fromInterval == untilInterval {
		untilInterval = untilInterval.Add(step)
	}

	points := w.fetchRawPoints(retentionID, fromInterval, untilInterval)
	// log.Printf("FetchFromArchive after fetchRawPoints, retentionID=%d, len(points)=%d", retentionID, len(points))
	// for i, pt := range points {
	// 	log.Printf("rawPoint i=%d, time=%s, value=%s", i, pt.Time, pt.Value)
	// }
	clearOldPoints(points, fromInterval, step)
	// log.Printf("FetchFromArchive after clearOldPoints, retentionID=%d, len(points)=%d", retentionID, len(points))

	return &TimeSeries{
		fromTime:  fromInterval,
		untilTime: untilInterval,
		step:      step,
		points:    points,
	}, nil
}

func (w *Whisper) findBestRetention(t, now Timestamp) int {
	var retentionID int
	diff := now.Sub(t)
	for i, retention := range w.Retentions() {
		retentionID = i
		if retention.MaxRetention() >= diff {
			break
		}
	}
	return retentionID
}

// Update a value in the database.
//
// If the timestamp is in the future or outside of the maximum retention it will
// fail immediately.
func (w *Whisper) Update(t Timestamp, v Value) error {
	return w.UpdatePointForArchive(RetentionIDBest, t, v, 0)
}

// UpdatePointForArchive updates one point in the specified archive.
func (w *Whisper) UpdatePointForArchive(retentionID int, t Timestamp, v Value, now Timestamp) error {
	if now == 0 {
		now = TimestampFromStdTime(time.Now())
	}

	if t <= now.Add(-w.MaxRetention()) || now < t {
		return fmt.Errorf("Timestamp not covered by any archives in this database")
	}

	if retentionID == RetentionIDBest {
		retentionID = w.findBestRetention(t, now)
	}

	r := &w.retentions[retentionID]
	myInterval := r.intervalForWrite(t)
	offset := w.getPointOffset(myInterval, r)
	pt := Point{Time: myInterval, Value: v}
	w.putPointAt(pt, offset)

	alignedPoints := []Point{pt}
	if err := w.propagateChain(retentionID, alignedPoints, now); err != nil {
		return err
	}
	return nil
}

// UpdateMany updates points in the best matching archives.
func (w *Whisper) UpdateMany(points []Point) (err error) {
	return w.UpdatePointsForArchive(points, RetentionIDBest, 0)
}

// UpdatePointsForArchive updates points in the specified archive.
// If retentionID is RetentionIDBest, points are first splitted for
// the best matching archives and update the corresponding archives.
func (w *Whisper) UpdatePointsForArchive(points []Point, retentionID int, now Timestamp) error {
	if now == 0 {
		now = TimestampFromStdTime(time.Now())
	}

	log.Printf("UpdatePointsForArchive start, points=%s, retentionID=%d, now=%s", points, retentionID, now)
	sort.Stable(Points(points))
	for retID, r := range w.Retentions() {
		// log.Printf("start retentionID=%d", retentionID)
		if retentionID != RetentionIDBest && retentionID != retID {
			// log.Printf("skip retentionID=%d", retentionID)
			continue
		}

		var currentPoints Points
		currentPoints, points = extractPoints(points, now, r.MaxRetention())
		if len(currentPoints) == 0 {
			// log.Printf("skip because len(currentPoints)==0")
			continue
		}

		// log.Printf("calling archiveUpdateMany currentPoints=%s, len(currentPoints)=%d, retID=%d, now=%s", currentPoints, len(currentPoints), retID, now)
		if err := w.archiveUpdateMany(currentPoints, retID, now); err != nil {
			return err
		}
	}
	return nil
}

func (w *Whisper) archiveUpdateMany(points []Point, retentionID int, now Timestamp) error {
	r := &w.retentions[retentionID]
	alignedPoints := r.alignPoints(points)

	log.Printf("archiveUpdateMany retentionID=%d, offset=%d, offsetEnd=%d", retentionID, r.offset, r.offset+r.numberOfPoints*pointSize)
	baseInterval := w.baseInterval(r)
	if baseInterval == 0 {
		baseInterval = r.intervalForWrite(now)
		log.Printf("archiveUpdateMany baseInterval was 0 now %s", baseInterval)
	} else {
		log.Printf("archiveUpdateMany baseInterval is %s", baseInterval)
	}

	for _, p := range alignedPoints {
		offset := r.pointOffsetAt(r.pointIndex(baseInterval, p.Time))
		log.Printf("archiveUpdateMany putPoint retentionID=%d, time=%s, value=%s, offset=%d", retentionID, p.Time, p.Value, offset)
		if offset >= r.offset+r.numberOfPoints*pointSize {
			log.Printf("archiveUpdateMany point offset is after offsetEnd, offset=%d, offsetEnd=%d", offset, r.offset+r.numberOfPoints*pointSize)
		}
		w.putPointAt(p, offset)
	}

	if err := w.propagateChain(retentionID, alignedPoints, now); err != nil {
		return err
	}
	return nil
}

// extractPoints extract points for the current archive.
// It returns points whose time is greater than or equal to now.Add(-maxRetention).
// Note: points must be sorted in ascending time order.
func extractPoints(points []Point, now Timestamp, maxRetention Duration) (currentPoints Points, remainingPoints Points) {
	maxAge := now.Add(-maxRetention)
	// log.Printf("extractPoints now=%s, maxRetention=%s, maxAge=%s", now, maxRetention, maxAge)
	for i := len(points) - 1; i >= 0; i-- {
		p := points[i]
		if p.Time <= maxAge {
			if i > 0 {
				return points[i+1:], points[:i+1]
			} else {
				return Points{}, points
			}
		}
	}
	return points, remainingPoints
}

func (w *Whisper) propagateChain(retentionID int, alignedPoints []Point, now Timestamp) error {
	log.Printf("propagateChain start, retentionID=%d, alignedPoints=%s, now=%s", retentionID, alignedPoints, now)
	lowRetID := retentionID + 1
	if lowRetID < len(w.retentions) {
		rLow := &w.retentions[lowRetID]
		ts := rLow.timesToPropagate(alignedPoints)
		log.Printf("propagateChain timesToPropagate result=%s", ts)
		for ; lowRetID < len(w.retentions) && len(ts) > 0; lowRetID++ {
			var err error
			ts, err = w.propagate(lowRetID, ts, now)
			log.Printf("propagateChain after propagate ts=%s, err=%v", ts, err)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Whisper) initNewBuf(retentions []Retention, aggregationMethod AggregationMethod, xFilesFactor float32) error {
	m := meta{
		aggregationMethod: aggregationMethod,
		xFilesFactor:      xFilesFactor,
	}
	if err := validateMetaAndRetentions(m, retentions); err != nil {
		return err
	}
	w.meta = m
	w.retentions = retentions
	w.fillDerivedValuesInHeader()
	w.buf = make([]byte, w.fileSizeFromHeader())
	w.setPagesDirtyByOffsetRange(0, uint32(len(w.buf)))
	w.putMeta()
	w.putRetentions()
	return nil
}

func (w *Whisper) readMetaAndRetentions() error {
	expectedSize := metaSize
	if len(w.buf) < expectedSize {
		//log.Printf("buf size is smaller than metaSize, bufSize=%d, metaSize=%d", len(w.buf), metaSize)
		return io.ErrUnexpectedEOF
	}

	w.meta.aggregationMethod = AggregationMethod(w.uint32At(0))
	w.meta.maxRetention = Duration(w.uint32At(uint32Size))
	w.meta.xFilesFactor = w.float32At(2 * uint32Size)

	w.meta.retentionCount = w.uint32At(3 * uint32Size)
	if w.meta.retentionCount == 0 {
		return errors.New("retention count must not be zero")
	}

	expectedSize += int(w.meta.retentionCount) * retentionSize
	if len(w.buf) < expectedSize {
		//log.Printf("buf size is smaller than retentionEnd, bufSize=%d, expectedSize=%d", len(w.buf), expectedSize)
		return io.ErrUnexpectedEOF
	}

	w.retentions = make([]Retention, w.meta.retentionCount)
	off := uint32(metaSize)
	for i := 0; i < int(w.meta.retentionCount); i++ {
		r := &w.retentions[i]
		r.offset = w.uint32At(off)
		if int(r.offset) != expectedSize {
			return fmt.Errorf("unexpected offset for retention %d, got: %d, want: %d", i, r.offset, expectedSize)
		}
		r.secondsPerPoint = Duration(w.uint32At(off + uint32Size))
		r.numberOfPoints = w.uint32At(off + 2*uint32Size)

		off += retentionSize
		expectedSize += int(r.numberOfPoints) * pointSize
	}

	if len(w.buf) < expectedSize {
		//log.Printf("buf size is smaller than expected EOF, bufSize=%d, expectedSize=%d", len(w.buf), expectedSize)
		return io.ErrUnexpectedEOF
	} else if len(w.buf) > expectedSize {
		w.buf = w.buf[:expectedSize]
	}

	if w.meta.maxRetention != w.retentions[len(w.retentions)-1].MaxRetention() {
		return errors.New("maxRetention in meta unmatch to maxRetention of lowest retention")
	}

	if err := validateMetaAndRetentions(w.meta, w.retentions); err != nil {
		return err
	}

	return nil
}

func (w *Whisper) baseInterval(r *Retention) Timestamp {
	return w.timestampAt(r.offset)
}

func (w *Whisper) pointAt(offset uint32) Point {
	return Point{
		Time:  w.timestampAt(offset),
		Value: w.valueAt(offset + uint32Size),
	}
}

func (w *Whisper) timestampAt(offset uint32) Timestamp {
	return Timestamp(w.uint32At(offset))
}

func (w *Whisper) valueAt(offset uint32) Value {
	return Value(w.float64At(offset))
}

func (w *Whisper) float32At(offset uint32) float32 {
	return math.Float32frombits(w.uint32At(offset))
}

func (w *Whisper) float64At(offset uint32) float64 {
	return math.Float64frombits(w.uint64At(offset))
}

func (w *Whisper) uint32At(offset uint32) uint32 {
	return binary.BigEndian.Uint32(w.buf[offset:])
}

func (w *Whisper) uint64At(offset uint32) uint64 {
	return binary.BigEndian.Uint64(w.buf[offset:])
}

func (w *Whisper) putPointAt(p Point, offset uint32) {
	w.putTimestampAt(p.Time, offset)
	w.putValueAt(p.Value, offset+uint32Size)
}

func (w *Whisper) putTimestampAt(t Timestamp, offset uint32) {
	w.putUint32At(uint32(t), offset)
}

func (w *Whisper) putValueAt(v Value, offset uint32) {
	w.putFloat64At(float64(v), offset)
}

func (w *Whisper) putFloat32At(v float32, offset uint32) {
	w.putUint32At(math.Float32bits(v), offset)
}

func (w *Whisper) putFloat64At(v float64, offset uint32) {
	w.putUint64At(math.Float64bits(v), offset)
}

func (w *Whisper) putUint32At(v uint32, offset uint32) {
	w.setPagesDirtyByOffsetRange(offset, uint32Size)
	binary.BigEndian.PutUint32(w.buf[offset:], v)
}

func (w *Whisper) putUint64At(v uint64, offset uint32) {
	w.setPagesDirtyByOffsetRange(offset, uint64Size)
	binary.BigEndian.PutUint64(w.buf[offset:], v)
}

func (w *Whisper) setPagesDirtyByOffsetRange(offset, size uint32) {
	startPage := pageForStartOffset(offset)
	endPage := pageForEndOffset(offset + size)
	for page := startPage; page <= endPage; page++ {
		w.setPageDirty(page)
	}
}

func (w *Whisper) initDirtyPageBitSet() {
	pageCount := (len(w.buf) + pageSize - 1) / pageSize
	w.dirtyPageBitSet = bitset.New(uint(pageCount))
}

func (w *Whisper) setPageDirty(page int) {
	if w.dirtyPageBitSet == nil {
		w.initDirtyPageBitSet()
	}
	w.dirtyPageBitSet.Set(uint(page))
}

func pageForStartOffset(offset uint32) int {
	return int(offset) / pageSize
}

func pageForEndOffset(offset uint32) int {
	return int(offset-1) / pageSize
}

func (w *Whisper) flushTo(wr io.WriterAt) error {
	for _, r := range dirtyPageRanges(w.dirtyPageBitSet) {
		off := r.start * pageSize
		end := r.end * pageSize
		if end > len(w.buf) {
			end = len(w.buf)
		}
		if _, err := wr.WriteAt(w.buf[off:end], int64(off)); err != nil {
			return err
		}
	}
	w.dirtyPageBitSet.ClearAll()
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

// GetAllRawUnsortedPoints returns the raw unsorted points.
// This is provided for the debugging or investination purpose.
func (w *Whisper) GetAllRawUnsortedPoints(retentionID int) []Point {
	r := &w.retentions[retentionID]
	points := make([]Point, r.numberOfPoints)
	off := r.offset
	for i := 0; i < len(points); i++ {
		points[i] = w.pointAt(off)
		off += pointSize
	}
	return points
}

func (w *Whisper) fetchRawPoints(retentionID int, fromInterval, untilInterval Timestamp) []Point {
	r := &w.retentions[retentionID]
	baseInterval := w.baseInterval(r)

	step := r.secondsPerPoint
	points := make([]Point, untilInterval.Sub(fromInterval)/step)
	log.Printf("fetchRawPoints, retentionID=%d, fromInterval=%s, untilInterval=%s, step=%s, len(points)=%d", retentionID, fromInterval, untilInterval, step, len(points))

	fromOffset := r.pointOffsetAt(r.pointIndex(baseInterval, fromInterval))
	untilOffset := r.pointOffsetAt(r.pointIndex(baseInterval, untilInterval))
	if fromOffset < untilOffset {
		log.Printf("fetchRawPoints case fromOffset < untilOffset, fromOffset=%d, untilOffset=%d", fromOffset, untilOffset)
		i := 0
		for off := fromOffset; off < untilOffset; off += pointSize {
			points[i] = w.pointAt(off)
			// log.Printf("fetchRawPoints case fromOffset < untilOffset, points[%d]=%s", i, points[i])
			i++
		}
		return points
	}

	arcStartOffset := r.offset
	arcEndOffset := arcStartOffset + r.numberOfPoints*pointSize
	log.Printf("fetchRawPoints case fromOffset >= untilOffset, fromOffset=%d, untilOffset=%d, start=%d, end=%d", fromOffset, untilOffset, arcStartOffset, arcEndOffset)

	i := 0
	for off := fromOffset; off < arcEndOffset; off += pointSize {
		points[i] = w.pointAt(off)
		// log.Printf("fetchRawPoints case fromOffset >= untilOffset, 1st part, points[%d]=%s", i, points[i])
		i++
	}
	for off := arcStartOffset; off < untilOffset; off += pointSize {
		points[i] = w.pointAt(off)
		// log.Printf("fetchRawPoints case fromOffset >= untilOffset, 2nd part, points[%d]=%s", i, points[i])
		i++
	}
	return points
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

// PrintHeader prints the header information to the writer in LTSV format [1].
// [1] Labeled Tab-separated Values http://ltsv.org/
func (w *Whisper) PrintHeader(wr io.Writer) error {
	m := &w.meta
	_, err := fmt.Fprintf(wr, "aggMethod:%s\taggMethodNum:%d\tmaxRetention:%s\txFileFactor:%s\tretentionCount:%d\n",
		m.aggregationMethod,
		int(m.aggregationMethod),
		m.maxRetention,
		strconv.FormatFloat(float64(m.xFilesFactor), 'f', -1, 32),
		m.retentionCount)
	if err != nil {
		return err
	}

	for i := range w.retentions {
		r := &w.retentions[i]
		_, err := fmt.Fprintf(wr, "retentionDef:%d\tstep:%s\tnumberOfPoints:%d\toffset:%d\n",
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

func (w *Whisper) fillDerivedValuesInHeader() {
	w.meta.maxRetention = w.retentions[len(w.retentions)-1].MaxRetention()
	w.meta.retentionCount = uint32(len(w.retentions))
	off := metaSize + len(w.retentions)*retentionSize
	for i := range w.retentions {
		r := &w.retentions[i]
		r.offset = uint32(off)
		off += int(r.numberOfPoints) * pointSize
	}
}

func (w *Whisper) fileSizeFromHeader() int64 {
	sz := int64(metaSize)
	for i := range w.retentions {
		r := &w.retentions[i]
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

func (w *Whisper) putMeta() {
	w.putUint32At(uint32(w.meta.aggregationMethod), 0)
	w.putUint32At(uint32(w.meta.maxRetention), uint32Size)
	w.putFloat32At(w.meta.xFilesFactor, 2*uint32Size)
	w.putUint32At(uint32(w.meta.retentionCount), 3*uint32Size)
}

func (w *Whisper) putRetentions() {
	off := uint32(metaSize)
	for i := range w.retentions {
		r := &w.retentions[i]
		w.putUint32At(r.offset, off)
		w.putUint32At(uint32(r.secondsPerPoint), off+uint32Size)
		w.putUint32At(r.numberOfPoints, off+2*uint32Size)
		off += retentionSize
	}
}

func (w *Whisper) propagate(retentionID int, ts []Timestamp, now Timestamp) (propagatedTs []Timestamp, err error) {
	log.Printf("propagate start, retentionID=%d, ts=%s, now=%s", retentionID, ts, now)
	if len(ts) == 0 {
		return nil, nil
	}

	r := &w.retentions[retentionID]
	baseInterval := w.baseInterval(r)
	if baseInterval == 0 {
		baseInterval = r.intervalForWrite(now)
	}

	step := r.secondsPerPoint
	highRetID := retentionID - 1
	var rLow *Retention
	if retentionID+1 < len(w.retentions) {
		rLow = &w.retentions[retentionID+1]
	}

	for _, t := range ts {
		fromInterval := t
		untilInterval := t.Add(step)
		points := w.fetchRawPoints(highRetID, fromInterval, untilInterval)
		log.Printf("fetchRawPoints result points=%s, highRetID=%d, highRet=%s, retID=%d, ret=%s, fromInterval=%s, untilInterval=%s", points, highRetID, w.Retentions()[highRetID], retentionID, r, fromInterval, untilInterval)
		values := filterValidValues(points, fromInterval, &w.Retentions()[highRetID])
		knownFactor := float32(len(values)) / float32(len(points))
		log.Printf("propagate values=%s, knownFactor=%s (=%d/%d), w.meta.xFilesFactor=%s",
			values,
			strconv.FormatFloat(float64(knownFactor), 'f', -1, 32),
			len(values),
			len(points),
			strconv.FormatFloat(float64(w.meta.xFilesFactor), 'f', -1, 32))
		if knownFactor < w.meta.xFilesFactor {
			continue
		}

		v := aggregate(w.meta.aggregationMethod, values)
		offset := w.getPointOffset(t, r)
		log.Printf("propagate v=%s, t=%s, offset=%d, retentionID=%d, retOffStart=%d, retOffEnd=%d", v, t, offset, retentionID, r.offset, r.offset+r.numberOfPoints*pointSize)
		w.putTimestampAt(t, offset)
		w.putValueAt(v, offset+uint32Size)

		if rLow != nil {
			tLow := rLow.intervalForWrite(t)
			if len(propagatedTs) == 0 || propagatedTs[len(propagatedTs)-1] != tLow {
				propagatedTs = append(propagatedTs, tLow)
			}
		}
	}

	return propagatedTs, nil
}

func (whisper *Whisper) getPointOffset(start Timestamp, r *Retention) uint32 {
	baseInterval := whisper.baseInterval(r)
	if baseInterval == 0 {
		return r.offset
	}
	return r.pointOffsetAt(r.pointIndex(baseInterval, start))
}

func filterValidValues(points []Point, fromInterval Timestamp, rLow *Retention) []Value {
	values := make([]Value, 0, len(points))
	currentInterval := rLow.intervalForWrite(fromInterval)
	for _, p := range points {
		if p.Time == currentInterval {
			// log.Printf("filterValidValues add p.Time=%s, p.Value=%s", p.Time, p.Value)
			values = append(values, p.Value)
		}
		currentInterval = currentInterval.Add(rLow.SecondsPerPoint())
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
