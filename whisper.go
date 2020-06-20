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
	"strings"
	"time"
)

type pageID uint64
type pageIDSlice []pageID

type Whisper struct {
	Meta       Meta
	Retentions []Retention

	bufPool  *BufferPool
	pageSize int64

	file         *os.File
	fileSize     int64
	lastPageID   pageID
	lastPageSize int64

	pages      map[pageID][]byte
	dirtyPages map[pageID]struct{}

	baseIntervals []Timestamp
}

type Meta struct {
	AggregationMethod AggregationMethod
	maxRetention      Duration
	XFilesFactor      float32
	retentionCount    uint32
}

type Retentions []Retention

type Retention struct {
	offset          uint32
	SecondsPerPoint Duration
	NumberOfPoints  uint32
}

type Point struct {
	Time  Timestamp
	Value float64
}

type pageAndOffset struct {
	pgid pageID
	off  int64
}

type pageAndOffsetRange struct {
	start pageAndOffset
	end   pageAndOffset
}

// These are sizes in whisper files.
// NOTE: The size of type Point is different from the size of
// point in file since Timestamps is int64, not uint32.
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

func (w *Whisper) OpenOrCreate(filename string, bufPool *BufferPool, perm os.FileMode) error {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, perm)
	if err != nil {
		return err
	}

	w.file = file
	w.bufPool = bufPool

	err = w.readHeader()
	if err == nil {
		return nil
	}
	if err != io.ErrUnexpectedEOF {
		return err
	}

	if err := Retentions(w.Retentions).validate(); err != nil {
		return err
	}

	w.fillDerivedValuesInHeader()
	w.initPages(w.fileSizeFromHeader())
	for i := pageID(0); i <= w.lastPageID; i++ {
		w.pages[i] = w.allocBuf(i)
		w.markPageDirty(i)
	}
	w.putMeta()
	w.putRetentions()

	return nil
}

func Open(filename string, bufPool *BufferPool) (*Whisper, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	w := &Whisper{
		file:    file,
		bufPool: bufPool,
	}

	if err := w.readHeader(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Whisper) initPages(fileSize int64) {
	w.fileSize = fileSize
	w.pageSize = int64(w.bufPool.BufferSize())
	w.lastPageID = pageID(w.fileSize / w.pageSize)
	w.lastPageSize = w.fileSize % w.pageSize
	w.pages = make(map[pageID][]byte)
	w.dirtyPages = make(map[pageID]struct{})
}

func (w *Whisper) readHeader() error {
	st, err := w.file.Stat()
	if err != nil {
		return err
	}
	w.initPages(st.Size())

	if w.fileSize < metaSize {
		return io.ErrUnexpectedEOF
	}
	if err := w.readMeta(); err != nil {
		return err
	}

	if w.fileSize < metaSize+int64(w.Meta.retentionCount)*retentionSize {
		return io.ErrUnexpectedEOF
	}
	if err := w.readRetentions(); err != nil {
		return err
	}
	w.initBaseIntervals()
	return nil
}

func (w *Whisper) Close() error {
	for _, page := range w.pages {
		w.bufPool.Put(page)
	}
	return w.file.Close()
}

func (w *Whisper) readMeta() error {
	//log.Printf("readMeta start, before readPagesIfNeeded")
	if err := w.readPagesIfNeeded(0, 0); err != nil {
		return err
	}

	w.Meta.AggregationMethod = AggregationMethod(w.uint32At(0))
	w.Meta.maxRetention = Duration(w.uint32At(uint32Size))
	w.Meta.XFilesFactor = w.float32At(2 * uint32Size)
	w.Meta.retentionCount = w.uint32At(3 * uint32Size)
	return nil
}

func (w *Whisper) readRetentions() error {
	nRet := int64(w.Meta.retentionCount)
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
	w.baseIntervals = make([]Timestamp, len(w.Retentions))
	for i := range w.baseIntervals {
		w.baseIntervals[i] = -1
	}
}

func (w *Whisper) baseInterval(retentionID int) (Timestamp, error) {
	interval := w.baseIntervals[retentionID]
	if interval != -1 {
		return interval, nil
	}

	r := w.Retentions[retentionID]
	off := int64(r.offset)
	fromPg := pageID(off / w.pageSize)
	untilPg := pageID((off + uint32Size) / w.pageSize)
	if err := w.readPagesIfNeeded(fromPg, untilPg); err != nil {
		return 0, fmt.Errorf("cannot read page from %d to %d for baseInterval: %s",
			fromPg, untilPg, err)
	}
	return w.timestampAt(off), nil
}

func (w *Whisper) readPagesIfNeeded(from, until pageID) error {
	//log.Printf("readPagesIfNeeded start, from=%d, until=%d", from, until)
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
		//log.Printf("readPagesIfNeeded before preadvFull from=%d, to=%d, off=%d", from, pid-1, off)
		_, err := preadvFull(w.file, iovs, off)
		//log.Printf("readPagesIfNeeded after preadvFull from=%d, to=%d, off=%d, n=%d, err=%v", from, pid-1, off, n, err)
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

func (w *Whisper) Flush() error {
	pgids := make([]pageID, 0, len(w.dirtyPages))
	for pgid := range w.dirtyPages {
		pgids = append(pgids, pgid)
	}
	sort.Sort(pageIDSlice(pgids))

	var off int64
	var iovs [][]byte
	for i, pgid := range pgids {
		if i == 0 || pgids[i-1] != pgid-1 {
			off = int64(pgid) * w.pageSize
			iovs = [][]byte{w.pages[pgid]}
		} else {
			iovs = append(iovs, w.pages[pgid])
		}

		if i == len(pgids)-1 || pgids[i+1] != pgid+1 {
			_, err := pwritevFull(w.file, iovs, off)
			if err != nil {
				return err
			}
		}
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	w.dirtyPages = make(map[pageID]struct{})
	return nil
}

func (w *Whisper) markPageDirty(pgid pageID) {
	w.dirtyPages[pgid] = struct{}{}
}

func (w *Whisper) fillDerivedValuesInHeader() {
	w.Meta.maxRetention = w.Retentions[len(w.Retentions)-1].MaxRetention()
	w.Meta.retentionCount = uint32(len(w.Retentions))
	off := metaSize + len(w.Retentions)*retentionSize
	for i := range w.Retentions {
		r := &w.Retentions[i]
		r.offset = uint32(off)
		off += int(r.NumberOfPoints) * pointSize
	}
}

func (w *Whisper) fileSizeFromHeader() int64 {
	sz := int64(metaSize)
	for _, r := range w.Retentions {
		sz += retentionSize + int64(r.NumberOfPoints)*pointSize
	}
	return sz
}

func (w *Whisper) FetchFromSpecifiedArchive(retentionID int, from, until, now Timestamp) ([]Point, error) {
	if now == 0 {
		now = TimestampFromStdTime(time.Now())
	}
	//log.Printf("FetchFromSpecifiedArchive start, from=%s, until=%s, now=%s",
	//	from, until, now)
	if from > until {
		return nil, fmt.Errorf("invalid time interval: from time '%d' is after until time '%d'", from, until)
	}
	oldest := now.Add(-w.Meta.maxRetention)
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
	//log.Printf("FetchFromSpecifiedArchive adjusted, from=%s, until=%s, now=%s", from, until, now)

	if retentionID < 0 || len(w.Retentions)-1 < retentionID {
		return nil, ErrRetentionIDOutOfRange
	}

	baseInterval, err := w.baseInterval(retentionID)
	//log.Printf("FetchFromSpecifiedArchive baseInterval=%s, err=%v", baseInterval, err)
	if err != nil {
		return nil, err
	}

	r := &w.Retentions[retentionID]
	fromInterval := r.Interval(from)
	untilInterval := r.Interval(until)
	step := Timestamp(r.SecondsPerPoint)

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
			i, pt.Time, formatFloat64(pt.Value))
	}
	clearOldPoints(points, fromInterval, step)

	return points, nil
}

func (w *Whisper) fetchRawPoints(fromInterval, untilInterval Timestamp, retentionID int) ([]Point, error) {
	r := &w.Retentions[retentionID]
	baseInterval, err := w.baseInterval(retentionID)
	if err != nil {
		return nil, err
	}
	log.Printf("fetchRawPoints, baseInterval=%s, fromInterval=%s, untilInterval=%s", baseInterval, fromInterval, untilInterval)

	fromOffset := r.pointOffsetAt(r.pointIndex(baseInterval, fromInterval))
	untilOffset := r.pointOffsetAt(r.pointIndex(baseInterval, untilInterval))
	log.Printf("fetchRawPoints, fromOffset=%d, untilOffset=%d", fromOffset, untilOffset)

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

	step := Timestamp(r.SecondsPerPoint)
	points := make([]Point, (untilInterval-fromInterval)/step)

	retentionStartOffset := int64(r.offset)
	retentionEndOffset := retentionStartOffset + int64(r.NumberOfPoints)*pointSize
	log.Printf("fetchRawPoints, retentionStartOffset=%d, retentionEndOffset=%d, numberOfPoints=%d", retentionStartOffset, retentionEndOffset, r.NumberOfPoints)

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

func clearOldPoints(points []Point, fromInterval, step Timestamp) {
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
	off := int64(r.offset)
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
	copy(buf[size-overflow:], p1[:overflow])
	return buf
}

func (w *Whisper) markPagesDirty(offset, size int64) {
	startPgID := pageID(offset / w.pageSize)
	endPgID := pageID((offset + size) / w.pageSize)
	endOffInPg := (offset + size) % w.pageSize
	if endOffInPg == 0 {
		endPgID--
	}

	for i := startPgID; i <= endPgID; i++ {
		w.dirtyPages[i] = struct{}{}
	}
}

func (w *Whisper) retentionAt(offset int64) Retention {
	return Retention{
		offset:          w.uint32At(offset),
		SecondsPerPoint: Duration(w.uint32At(offset + uint32Size)),
		NumberOfPoints:  w.uint32At(offset + 2*uint32Size),
	}
}

func (w *Whisper) timestampAt(offset int64) Timestamp {
	return Timestamp(w.uint32At(offset))
}

func (w *Whisper) putUint32At(v uint32, offset int64) {
	r := w.pageAndOffsetRangeFromFileOffsetAndSize(offset, uint32Size)
	if r.fitsInOnePage() {
		binary.BigEndian.PutUint32(w.pages[r.start.pgid][r.start.off:], v)
		w.markPageDirty(r.start.pgid)
		return
	}
	var buf [uint32Size]byte
	binary.BigEndian.PutUint32(buf[:], v)
	w.splitCopyToPages(&r, buf[:])
}

func (w *Whisper) putUint64At(v uint64, offset int64) {
	r := w.pageAndOffsetRangeFromFileOffsetAndSize(offset, uint64Size)
	if r.fitsInOnePage() {
		binary.BigEndian.PutUint64(w.pages[r.start.pgid][r.start.off:], v)
		w.markPageDirty(r.start.pgid)
		return
	}
	var buf [uint64Size]byte
	binary.BigEndian.PutUint64(buf[:], v)
	w.splitCopyToPages(&r, buf[:])
}

func (w *Whisper) putFloat32At(v float32, offset int64) {
	w.putUint32At(math.Float32bits(v), offset)
}

func (w *Whisper) putFloat64At(v float64, offset int64) {
	w.putUint64At(math.Float64bits(v), offset)
}

func (w *Whisper) putTimestampAt(t Timestamp, offset int64) {
	w.putUint64At(uint64(t), offset)
}

func (w *Whisper) putMeta() {
	w.putUint32At(uint32(w.Meta.AggregationMethod), 0)
	w.putUint32At(uint32(w.Meta.maxRetention), uint32Size)
	w.putFloat32At(w.Meta.XFilesFactor, 2*uint32Size)
	w.putUint32At(uint32(w.Meta.retentionCount), 3*uint32Size)
}

func (w *Whisper) putRetentions() {
	off := int64(metaSize)
	for i := range w.Retentions {
		r := &w.Retentions[i]
		w.putUint32At(r.offset, off)
		w.putUint32At(uint32(r.SecondsPerPoint), off+uint32Size)
		w.putUint32At(r.NumberOfPoints, off+2*uint32Size)
		off += retentionSize
	}
}

func (w *Whisper) splitCopyToPages(r *pageAndOffsetRange, b []byte) {
	sz := w.pageSize - r.start.off
	copy(w.pages[r.start.pgid][r.start.off:], b[:sz])
	copy(w.pages[r.end.pgid][:r.end.off], b[sz:sz+r.end.off])
	w.markPageDirty(r.start.pgid)
	w.markPageDirty(r.end.pgid)
}

func (w *Whisper) pageAndOffsetFromFileOffset(offset int64) pageAndOffset {
	return pageAndOffset{
		pgid: pageID(offset / w.pageSize),
		off:  offset % w.pageSize,
	}
}

func (w *Whisper) pageAndOffsetRangeFromFileOffsetAndSize(offset, size int64) pageAndOffsetRange {
	return pageAndOffsetRange{
		start: w.pageAndOffsetFromFileOffset(offset),
		end:   w.pageAndOffsetFromFileOffset(offset + size),
	}
}

func (r *pageAndOffsetRange) fitsInOnePage() bool {
	return r.start.pgid == r.end.pgid ||
		(r.start.pgid+1 == r.end.pgid && r.end.off == 0)
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
	pointDistance := int64(interval-baseInterval) / int64(r.SecondsPerPoint)
	return int(floorMod(pointDistance, int64(r.NumberOfPoints)))
}

func (r *Retention) MaxRetention() Duration {
	return r.SecondsPerPoint * Duration(r.NumberOfPoints)
}

func (r *Retention) pointOffsetAt(index int) int64 {
	return int64(r.offset) + int64(index)*pointSize
}

func (r *Retention) Interval(t Timestamp) Timestamp {
	step := int64(r.SecondsPerPoint)
	return Timestamp(int64(t) - floorMod(int64(t), step) + step)
}

func formatFloat64(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func (pp pageIDSlice) Len() int           { return len(pp) }
func (pp pageIDSlice) Less(i, j int) bool { return pp[i] < pp[j] }
func (pp pageIDSlice) Swap(i, j int)      { pp[i], pp[j] = pp[j], pp[i] }
