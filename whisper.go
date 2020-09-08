package whispertool

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"syscall"
	"time"

	"github.com/hnakamur/filebuffer"
)

// Whisper represents a Whisper database file.
type Whisper struct {
	header Header

	file    *os.File
	fileBuf *filebuffer.FileBuffer

	openFileFlag int
	flock        bool
	perm         os.FileMode
	pageSize     int64
}

// Option is the type for options for creating or opening a whisper file.
type Option func(*Whisper)

// WithoutFlock disables flock for the file.
func WithoutFlock() Option {
	return func(w *Whisper) {
		w.flock = false
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
func Create(filename string, archiveInfoList []ArchiveInfo, aggregationMethod AggregationMethod, xFilesFactor float32, opts ...Option) (*Whisper, error) {
	h, err := NewHeader(aggregationMethod, xFilesFactor, archiveInfoList)
	if err != nil {
		return nil, err
	}

	w := &Whisper{
		header:       *h,
		openFileFlag: os.O_RDWR | os.O_CREATE | os.O_EXCL,
		flock:        true,
		perm:         0644,
		pageSize:     int64(os.Getpagesize()),
	}
	for _, opt := range opts {
		opt(w)
	}

	if err := w.openAndLockFile(filename); err != nil {
		return nil, err
	}

	fileSize := h.ExpectedFileSize()
	if err := w.file.Truncate(fileSize); err != nil {
		return nil, err
	}
	w.fileBuf = filebuffer.New(w.file, fileSize, w.pageSize)

	if err := w.putHeader(); err != nil {
		return nil, err
	}
	return w, nil
}

// Open opens an existing whisper database file.
// In the current implementation, the whole content is read after opening the file.
// This behavior may be changed in the future.
func Open(filename string, opts ...Option) (*Whisper, error) {
	w := &Whisper{
		openFileFlag: os.O_RDWR,
		flock:        true,
		perm:         0644,
		pageSize:     int64(os.Getpagesize()),
	}
	for _, opt := range opts {
		opt(w)
	}

	if err := w.openAndLockFile(filename); err != nil {
		return nil, err
	}

	st, err := w.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat: %s: %s", filename, err)
	}

	w.fileBuf = filebuffer.New(w.file, st.Size(), w.pageSize)

	if err := w.readHeader(); err != nil {
		return nil, fmt.Errorf("readHeader: %s: %s", filename, err)
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
			return fmt.Errorf("flock: %s %s", filename, err)
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
	if err := w.fileBuf.Flush(); err != nil {
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
	return w.file.Close()
}

// Header returns the header of the whisper file.
func (w *Whisper) Header() *Header { return &w.header }

// AggregationMethod returns the aggregation method of the whisper file.
func (w *Whisper) AggregationMethod() AggregationMethod { return w.Header().AggregationMethod() }

// XFilesFactor returns the xFilesFactor of the whisper file.
func (w *Whisper) XFilesFactor() float32 { return w.Header().XFilesFactor() }

// MaxRetention returns the max retention of the whisper file.
func (w *Whisper) MaxRetention() Duration { return w.Header().MaxRetention() }

// ArchiveInfoList returns the archive info list of the whisper file.
func (w *Whisper) ArchiveInfoList() ArchiveInfoList { return w.Header().ArchiveInfoList() }

// Fetch fetches points from the best archive for the specified time range.
//
// It fetches points in range between `from` (exclusive) and `until` (inclusive).
func (w *Whisper) Fetch(from, until Timestamp) (*TimeSeries, error) {
	return w.FetchFromArchive(ArchiveIDBest, from, until, 0)
}

// Now is a function which returns the current time.
// You can mock this function in tests.
var Now = time.Now

// FetchFromArchive fetches points in the specified archive and the time range.
//
// FetchFromArchive fetches points from archive specified with `arhiveID`.
// It fetches points in range between `from` (exclusive) and `until` (inclusive).
// If `now` is zero, the current time is used.
func (w *Whisper) FetchFromArchive(arhiveID int, from, until, now Timestamp) (*TimeSeries, error) {
	if now == 0 {
		now = TimestampFromStdTime(Now())
	}
	if from > until {
		return nil, fmt.Errorf("invalid time interval: from time '%d' is after until time '%d'", from, until)
	}
	if (arhiveID != ArchiveIDBest && arhiveID < 0) || len(w.ArchiveInfoList())-1 < arhiveID {
		return nil, ErrArchiveIDOutOfRange
	}
	if arhiveID == ArchiveIDBest {
		arhiveID = w.findBestArchive(from, now)
	}
	r := &w.ArchiveInfoList()[arhiveID]

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

	baseInterval, err := w.baseInterval(r)
	if err != nil {
		return nil, err
	}

	fromInterval := r.interval(from)
	untilInterval := r.interval(until)
	step := r.secondsPerPoint

	if baseInterval == 0 {
		values := make([]Value, (untilInterval-fromInterval)/Timestamp(step))
		for i := range values {
			values[i].SetNaN()
		}
		return &TimeSeries{
			fromTime:  fromInterval,
			untilTime: untilInterval,
			step:      step,
			values:    values,
		}, nil
	}

	// Zero-length time range: always include the next point
	if fromInterval == untilInterval {
		untilInterval = untilInterval.Add(step)
	}

	points, err := w.fetchRawPoints(arhiveID, fromInterval, untilInterval)
	if err != nil {
		return nil, err
	}
	clearOldPoints(points, fromInterval, step)

	return &TimeSeries{
		fromTime:  fromInterval,
		untilTime: untilInterval,
		step:      step,
		values:    points.Values(),
	}, nil
}

func (w *Whisper) findBestArchive(t, now Timestamp) int {
	var archiveID int
	diff := now.Sub(t)
	for i, retention := range w.ArchiveInfoList() {
		archiveID = i
		if retention.MaxRetention() >= diff {
			break
		}
	}
	return archiveID
}

// Update a value in the database.
//
// If the timestamp is in the future or outside of the maximum retention it will
// fail immediately.
func (w *Whisper) Update(t Timestamp, v Value) error {
	return w.UpdatePointForArchive(ArchiveIDBest, t, v, 0)
}

// UpdatePointForArchive updates one point in the specified archive.
func (w *Whisper) UpdatePointForArchive(archiveID int, t Timestamp, v Value, now Timestamp) error {
	// log.Printf("UpdatePointForArchive start, archiveID=%d, t=%s, v=%s, now=%s", archiveID, t, v, now)
	if now == 0 {
		now = TimestampFromStdTime(Now())
	}

	if t <= now.Add(-w.MaxRetention()) || now < t {
		return fmt.Errorf("Timestamp not covered by any archives in this database")
	}

	if archiveID == ArchiveIDBest {
		archiveID = w.findBestArchive(t, now)
		// log.Printf("UpdatePointForArchive best archiveID=%d", archiveID)
	}

	r := &w.ArchiveInfoList()[archiveID]
	myInterval := r.intervalForWrite(t)
	offset, err := w.getPointOffset(myInterval, r)
	// log.Printf("UpdatePointForArchive offest=%d, err=%v", offset, err)
	if err != nil {
		return err
	}
	pt := Point{Time: myInterval, Value: v}
	if err := w.putPointAt(pt, offset); err != nil {
		return err
	}

	alignedPoints := []Point{pt}
	if err := w.propagateChain(archiveID, alignedPoints, now); err != nil {
		return err
	}
	return nil
}

// UpdateMany updates points in the best matching archives.
func (w *Whisper) UpdateMany(points []Point) (err error) {
	return w.UpdatePointsForArchive(points, ArchiveIDBest, 0)
}

// UpdatePointsForArchive updates points in the specified archive.
//
// If archiveID is ArchiveIDBest, points are first splitted for
// the best matching archives and update the corresponding archives.
//
// Note this does NOT update points whose timestamp is out of
// the timestamp range of the specified archive.
// This behavior is not compatible to Whisper.UpdateMany in
// github.com/go-graphite/go-whisper.
func (w *Whisper) UpdatePointsForArchive(points []Point, archiveID int, now Timestamp) error {
	if now == 0 {
		now = TimestampFromStdTime(Now())
	}

	sort.Stable(Points(points))
	for retID, r := range w.ArchiveInfoList() {
		if archiveID != ArchiveIDBest && archiveID != retID {
			continue
		}

		var currentPoints Points
		currentPoints, points = extractPoints(points, now, r.MaxRetention())
		if len(currentPoints) == 0 {
			continue
		}

		if err := w.archiveUpdateMany(currentPoints, retID, now); err != nil {
			return err
		}
	}
	return nil
}

func (w *Whisper) archiveUpdateMany(points []Point, archiveID int, now Timestamp) error {
	r := &w.ArchiveInfoList()[archiveID]
	alignedPoints := r.alignPoints(points)

	baseInterval, err := w.baseInterval(r)
	if err != nil {
		return err
	}
	if baseInterval == 0 {
		baseInterval = alignedPoints[0].Time
	}

	for _, p := range alignedPoints {
		offset := r.pointOffsetAt(r.pointIndex(baseInterval, p.Time))
		if err := w.putPointAt(p, offset); err != nil {
			return err
		}
	}

	if err := w.propagateChain(archiveID, alignedPoints, now); err != nil {
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

func (w *Whisper) propagateChain(archiveID int, alignedPoints []Point, now Timestamp) error {
	lowRetID := archiveID + 1
	if lowRetID < len(w.ArchiveInfoList()) {
		rLow := &w.ArchiveInfoList()[lowRetID]
		ts := rLow.timesToPropagate(alignedPoints)
		for ; lowRetID < len(w.ArchiveInfoList()) && len(ts) > 0; lowRetID++ {
			var err error
			ts, err = w.propagate(lowRetID, ts, now)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Whisper) putHeader() error {
	buf := make([]byte, 0, w.pageSize)
	buf = w.header.AppendTo(buf)
	if _, err := w.fileBuf.WriteAt(buf[:w.header.Size()], 0); err != nil {
		return err
	}
	return nil
}

func (w *Whisper) readHeader() error {
	buf := make([]byte, w.pageSize)
	if _, err := w.fileBuf.ReadAt(buf[:metaSize], 0); err != nil {
		return err
	}

	h := &Header{}
	if _, err := h.TakeFrom(buf[:metaSize]); err != nil {
		var werr *WantLargerBufferError
		if !errors.As(err, &werr) {
			return err
		}

		wantSize := werr.WantedBufSize
		if wantSize > len(buf) {
			buf = make([]byte, wantSize)
		}
		if _, err := w.fileBuf.ReadAt(buf[:wantSize], 0); err != nil {
			return err
		}
		if _, err := h.TakeFrom(buf); err != nil {
			return err
		}
	}
	w.header = *h
	return nil
}

func (w *Whisper) baseInterval(a *ArchiveInfo) (Timestamp, error) {
	var buf [uint32Size]byte
	if _, err := w.fileBuf.ReadAt(buf[:], int64(a.offset)); err != nil {
		return 0, err
	}

	var t Timestamp
	if _, err := t.TakeFrom(buf[:]); err != nil {
		return 0, err
	}
	return t, nil
}

func (w *Whisper) readPointAt(offset uint32) (Point, error) {
	var buf [pointSize]byte
	if _, err := w.fileBuf.ReadAt(buf[:], int64(offset)); err != nil {
		return Point{}, err
	}

	var p Point
	if _, err := p.TakeFrom(buf[:]); err != nil {
		return Point{}, err
	}
	return p, nil
}

func (w *Whisper) putPointAt(p Point, offset uint32) error {
	var buf [pointSize]byte
	dest := p.AppendTo(buf[:0])
	// log.Printf("putPointAt p=%s, offset=%d, len(dest)=%d", p, offset, len(dest))
	if _, err := w.fileBuf.WriteAt(dest, int64(offset)); err != nil {
		return err
	}

	return nil
}

// GetAllRawUnsortedPoints returns the raw unsorted points.
// This is provided for the debugging or investination purpose.
func (w *Whisper) GetAllRawUnsortedPoints(archiveID int) (Points, error) {
	r := &w.ArchiveInfoList()[archiveID]
	points := make(Points, r.numberOfPoints)
	off := r.offset
	for i := 0; i < len(points); i++ {
		var err error
		points[i], err = w.readPointAt(off)
		if err != nil {
			return nil, err
		}
		off += pointSize
	}
	return points, nil
}

func (w *Whisper) fetchRawPoints(archiveID int, fromInterval, untilInterval Timestamp) (Points, error) {
	r := &w.ArchiveInfoList()[archiveID]
	baseInterval, err := w.baseInterval(r)
	if err != nil {
		return nil, err
	}

	step := r.secondsPerPoint
	points := make([]Point, untilInterval.Sub(fromInterval)/step)

	fromOffset := r.pointOffsetAt(r.pointIndex(baseInterval, fromInterval))
	untilOffset := r.pointOffsetAt(r.pointIndex(baseInterval, untilInterval))
	if fromOffset < untilOffset {
		i := 0
		for off := fromOffset; off < untilOffset; off += pointSize {
			points[i], err = w.readPointAt(off)
			if err != nil {
				return nil, err
			}
			i++
		}
		return points, nil
	}

	arcStartOffset := r.offset
	arcEndOffset := arcStartOffset + r.numberOfPoints*pointSize

	i := 0
	for off := fromOffset; off < arcEndOffset; off += pointSize {
		points[i], err = w.readPointAt(off)
		if err != nil {
			return nil, err
		}
		i++
	}
	for off := arcStartOffset; off < untilOffset; off += pointSize {
		points[i], err = w.readPointAt(off)
		if err != nil {
			return nil, err
		}
		i++
	}
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

func (w *Whisper) propagate(archiveID int, ts []Timestamp, now Timestamp) (propagatedTs []Timestamp, err error) {
	if len(ts) == 0 {
		return nil, nil
	}

	r := &w.ArchiveInfoList()[archiveID]
	baseInterval, err := w.baseInterval(r)
	if err != nil {
		return nil, err
	}
	if baseInterval == 0 {
		baseInterval = r.intervalForWrite(now)
	}

	step := r.secondsPerPoint
	highRetID := archiveID - 1
	rHigh := &w.ArchiveInfoList()[highRetID]
	var rLow *ArchiveInfo
	if archiveID+1 < len(w.ArchiveInfoList()) {
		rLow = &w.ArchiveInfoList()[archiveID+1]
	}

	for _, t := range ts {
		fromInterval := t
		untilInterval := t.Add(step)
		points, err := w.fetchRawPoints(highRetID, fromInterval, untilInterval)
		if err != nil {
			return nil, err
		}
		values := filterValidValues(points, fromInterval, rHigh)
		knownFactor := float32(len(values)) / float32(len(points))
		if knownFactor < w.XFilesFactor() {
			continue
		}

		v := aggregate(w.AggregationMethod(), values)
		offset, err := w.getPointOffset(t, r)
		if err != nil {

			return nil, err
		}
		p := Point{Time: t, Value: v}
		if err := w.putPointAt(p, offset); err != nil {
			return nil, err
		}

		if rLow != nil {
			tLow := rLow.intervalForWrite(t)
			if len(propagatedTs) == 0 || propagatedTs[len(propagatedTs)-1] != tLow {
				propagatedTs = append(propagatedTs, tLow)
			}
		}
	}

	return propagatedTs, nil
}

func (w *Whisper) getPointOffset(start Timestamp, a *ArchiveInfo) (uint32, error) {
	baseInterval, err := w.baseInterval(a)
	if err != nil {
		return 0, err
	}
	if baseInterval == 0 {
		return a.offset, nil
	}
	return a.pointOffsetAt(a.pointIndex(baseInterval, start)), nil
}

func filterValidValues(points []Point, fromInterval Timestamp, rLow *ArchiveInfo) []Value {
	values := make([]Value, 0, len(points))
	currentInterval := rLow.intervalForWrite(fromInterval)
	for _, p := range points {
		if p.Time == currentInterval {
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
