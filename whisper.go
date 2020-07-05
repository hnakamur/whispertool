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

	// log.Printf("Open filename=%s, before openAndLockFile", filename)
	if err := w.openAndLockFile(filename); err != nil {
		return nil, err
	}

	// log.Printf("Open filename=%s, before stat", filename)
	st, err := w.file.Stat()
	if err != nil {
		return nil, err
	}

	w.fileBuf = filebuffer.New(w.file, st.Size(), w.pageSize)

	// log.Printf("Open filename=%s, before readHeader", filename)
	if err := w.readHeader(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Whisper) openAndLockFile(filename string) error {
	// log.Printf("openAndLockFile start, filename=%s, openFlag=0x%x", filename, w.openFileFlag)
	file, err := os.OpenFile(filename, w.openFileFlag, w.perm)
	if err != nil {
		return err
	}
	w.file = file

	if w.flock {
		// log.Printf("openAndLockFile before flock, filename=%s", filename)
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

// FetchFromArchive fetches points in the specified archive and the time range.
//
// FetchFromArchive fetches points from archive specified with `retentionID`.
// It fetches points in range between `from` (exclusive) and `until` (inclusive).
// If `now` is zero, the current time is used.
func (w *Whisper) FetchFromArchive(retentionID int, from, until, now Timestamp) (*TimeSeries, error) {
	if now == 0 {
		now = TimestampFromStdTime(time.Now())
	}
	// log.Printf("FetchFromArchive start, from=%s, until=%s, now=%s", from, until, now)
	if from > until {
		return nil, fmt.Errorf("invalid time interval: from time '%d' is after until time '%d'", from, until)
	}
	if (retentionID != ArchiveIDBest && retentionID < 0) || len(w.ArchiveInfoList())-1 < retentionID {
		return nil, ErrArchiveIDOutOfRange
	}
	if retentionID == ArchiveIDBest {
		retentionID = w.findBestArchive(from, now)
		// log.Printf("found best retentionID=%d", retentionID)
	}
	r := &w.ArchiveInfoList()[retentionID]

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
	// log.Printf("FetchFromArchive adjusted, from=%s, until=%s, now=%s", from, until, now)

	baseInterval, err := w.baseInterval(r)
	if err != nil {
		return nil, err
	}
	// log.Printf("FetchFromArchive retentionID=%d, retention=%s, baseInterval=%s", retentionID, r, baseInterval)
	// log.Printf("FetchFromArchive retentions=%s", w.ArchiveInfoList()())

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

	points, err := w.fetchRawPoints(retentionID, fromInterval, untilInterval)
	if err != nil {
		return nil, err
	}
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
		values:    points.Values(),
	}, nil
}

func (w *Whisper) findBestArchive(t, now Timestamp) int {
	var retentionID int
	diff := now.Sub(t)
	for i, retention := range w.ArchiveInfoList() {
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
	return w.UpdatePointForArchive(ArchiveIDBest, t, v, 0)
}

// UpdatePointForArchive updates one point in the specified archive.
func (w *Whisper) UpdatePointForArchive(archiveID int, t Timestamp, v Value, now Timestamp) error {
	// log.Printf("UpdatePointForArchive start, archiveID=%d, t=%s, v=%s, now=%s", archiveID, t, v, now)
	if now == 0 {
		now = TimestampFromStdTime(time.Now())
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
// If retentionID is RetentionIDBest, points are first splitted for
// the best matching archives and update the corresponding archives.
func (w *Whisper) UpdatePointsForArchive(points []Point, retentionID int, now Timestamp) error {
	if now == 0 {
		now = TimestampFromStdTime(time.Now())
	}

	// log.Printf("UpdatePointsForArchive start, points=%s, retentionID=%d, now=%s", points, retentionID, now)
	sort.Stable(Points(points))
	for retID, r := range w.ArchiveInfoList() {
		// log.Printf("start retentionID=%d", retentionID)
		if retentionID != ArchiveIDBest && retentionID != retID {
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
	r := &w.ArchiveInfoList()[retentionID]
	alignedPoints := r.alignPoints(points)

	// log.Printf("archiveUpdateMany retentionID=%d, offset=%d, offsetEnd=%d", retentionID, r.offset, r.offset+r.numberOfPoints*pointSize)
	baseInterval, err := w.baseInterval(r)
	if err != nil {
		return err
	}
	if baseInterval == 0 {
		baseInterval = r.intervalForWrite(now)
	}

	for _, p := range alignedPoints {
		offset := r.pointOffsetAt(r.pointIndex(baseInterval, p.Time))
		// log.Printf("archiveUpdateMany putPoint retentionID=%d, time=%s, value=%s, offset=%d", retentionID, p.Time, p.Value, offset)
		// if offset >= r.offset+r.numberOfPoints*pointSize {
		// 	log.Printf("archiveUpdateMany point offset is after offsetEnd, offset=%d, offsetEnd=%d", offset, r.offset+r.numberOfPoints*pointSize)
		// }
		if err := w.putPointAt(p, offset); err != nil {
			return err
		}
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
	// log.Printf("propagateChain start, retentionID=%d, alignedPoints=%s, now=%s", retentionID, alignedPoints, now)
	lowRetID := retentionID + 1
	if lowRetID < len(w.ArchiveInfoList()) {
		rLow := &w.ArchiveInfoList()[lowRetID]
		ts := rLow.timesToPropagate(alignedPoints)
		// log.Printf("propagateChain timesToPropagate result=%s", ts)
		for ; lowRetID < len(w.ArchiveInfoList()) && len(ts) > 0; lowRetID++ {
			var err error
			ts, err = w.propagate(lowRetID, ts, now)
			// log.Printf("propagateChain after propagate ts=%s, err=%v", ts, err)
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
func (w *Whisper) GetAllRawUnsortedPoints(retentionID int) (Points, error) {
	r := &w.ArchiveInfoList()[retentionID]
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

func (w *Whisper) fetchRawPoints(retentionID int, fromInterval, untilInterval Timestamp) (Points, error) {
	r := &w.ArchiveInfoList()[retentionID]
	baseInterval, err := w.baseInterval(r)
	if err != nil {
		return nil, err
	}

	step := r.secondsPerPoint
	points := make([]Point, untilInterval.Sub(fromInterval)/step)
	// log.Printf("fetchRawPoints, retentionID=%d, fromInterval=%s, untilInterval=%s, step=%s, len(points)=%d", retentionID, fromInterval, untilInterval, step, len(points))

	fromOffset := r.pointOffsetAt(r.pointIndex(baseInterval, fromInterval))
	untilOffset := r.pointOffsetAt(r.pointIndex(baseInterval, untilInterval))
	if fromOffset < untilOffset {
		// log.Printf("fetchRawPoints case fromOffset < untilOffset, fromOffset=%d, untilOffset=%d", fromOffset, untilOffset)
		i := 0
		for off := fromOffset; off < untilOffset; off += pointSize {
			points[i], err = w.readPointAt(off)
			if err != nil {
				return nil, err
			}
			// log.Printf("fetchRawPoints case fromOffset < untilOffset, points[%d]=%s", i, points[i])
			i++
		}
		return points, nil
	}

	arcStartOffset := r.offset
	arcEndOffset := arcStartOffset + r.numberOfPoints*pointSize
	// log.Printf("fetchRawPoints case fromOffset >= untilOffset, fromOffset=%d, untilOffset=%d, start=%d, end=%d", fromOffset, untilOffset, arcStartOffset, arcEndOffset)

	i := 0
	for off := fromOffset; off < arcEndOffset; off += pointSize {
		points[i], err = w.readPointAt(off)
		if err != nil {
			return nil, err
		}
		// log.Printf("fetchRawPoints case fromOffset >= untilOffset, 1st part, points[%d]=%s", i, points[i])
		i++
	}
	for off := arcStartOffset; off < untilOffset; off += pointSize {
		points[i], err = w.readPointAt(off)
		if err != nil {
			return nil, err
		}
		// log.Printf("fetchRawPoints case fromOffset >= untilOffset, 2nd part, points[%d]=%s", i, points[i])
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

func (w *Whisper) propagate(retentionID int, ts []Timestamp, now Timestamp) (propagatedTs []Timestamp, err error) {
	// log.Printf("propagate start, retentionID=%d, ts=%s, now=%s", retentionID, ts, now)
	if len(ts) == 0 {
		return nil, nil
	}

	r := &w.ArchiveInfoList()[retentionID]
	baseInterval, err := w.baseInterval(r)
	if err != nil {
		return nil, err
	}
	if baseInterval == 0 {
		baseInterval = r.intervalForWrite(now)
	}

	step := r.secondsPerPoint
	highRetID := retentionID - 1
	var rLow *ArchiveInfo
	if retentionID+1 < len(w.ArchiveInfoList()) {
		rLow = &w.ArchiveInfoList()[retentionID+1]
	}

	for _, t := range ts {
		fromInterval := t
		untilInterval := t.Add(step)
		points, err := w.fetchRawPoints(highRetID, fromInterval, untilInterval)
		if err != nil {
			return nil, err
		}
		// log.Printf("fetchRawPoints result points=%s, highRetID=%d, highRet=%s, retID=%d, ret=%s, fromInterval=%s, untilInterval=%s", points, highRetID, w.ArchiveInfoList()()[highRetID], retentionID, r, fromInterval, untilInterval)
		values := filterValidValues(points, fromInterval, &w.ArchiveInfoList()[highRetID])
		knownFactor := float32(len(values)) / float32(len(points))
		// log.Printf("propagate values=%s, knownFactor=%s (=%d/%d), w.meta.xFilesFactor=%s",
		// 	values,
		// 	strconv.FormatFloat(float64(knownFactor), 'f', -1, 32),
		// 	len(values),
		// 	len(points),
		// 	strconv.FormatFloat(float64(w.meta.xFilesFactor), 'f', -1, 32))
		if knownFactor < w.XFilesFactor() {
			continue
		}

		v := aggregate(w.AggregationMethod(), values)
		offset, err := w.getPointOffset(t, r)
		if err != nil {

			return nil, err
		}
		// log.Printf("propagate v=%s, t=%s, offset=%d, retentionID=%d, retOffStart=%d, retOffEnd=%d", v, t, offset, retentionID, r.offset, r.offset+r.numberOfPoints*pointSize)
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
	// log.Printf("getPointOffset a=%s, baseInterval=%s, err=%v", a, baseInterval, a)
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
