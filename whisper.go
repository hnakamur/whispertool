package whispertool

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
)

type Whisper struct {
	Meta       Meta
	Retentions []Retention

	file *os.File
}

var ErrRetentionIDOutOfRange = errors.New("retention ID out of range")

//func (w *Whisper) Create(filename string, perm os.FileMode) error {
//	if err := Retentions(w.Retentions).validate(); err != nil {
//		return err
//	}
//
//	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, perm)
//	if err != nil {
//		return err
//	}
//
//	if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
//		file.Close()
//		return err
//	}
//
//	//w.file = file
//	//w.fillDerivedValuesInHeader()
//	//w.initBaseIntervals()
//	//w.initPages(w.fileSizeFromHeader())
//	//for i := int(0); i <= w.lastPageID; i++ {
//	//	w.pages[i] = w.allocBuf(i)
//	//	w.markPageDirty(i)
//	//}
//	//w.putMeta()
//	//w.putRetentions()
//	return nil
//}
//
//func OpenForWrite(filename string) (*Whisper, error) {
//	file, err := os.OpenFile(filename, os.O_RDWR, 0644)
//	if err != nil {
//		return nil, err
//	}
//
//	if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
//		file.Close()
//		return nil, err
//	}
//
//	w := &Whisper{
//		file: file,
//	}
//
//	if err := w.readHeader(); err != nil {
//		return nil, err
//	}
//	return w, nil
//}

func ReadFile(filename string) (*FileData, error) {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	d, err := NewFileData(buf)
	if err != nil {
		return nil, err
	}

	return d, nil
}

//func (w *Whisper) initPages(fileSize int64) {
//	w.fileSize = fileSize
//	w.pageSize = int64(w.bufPool.BufferSize())
//	w.lastPageID = int(w.fileSize / w.pageSize)
//	w.lastPageSize = w.fileSize % w.pageSize
//	w.pages = make(map[int][]byte)
//	w.dirtyPages = make(map[int]struct{})
//}
//
//func (w *Whisper) readHeader() error {
//	st, err := w.file.Stat()
//	if err != nil {
//		return err
//	}
//	w.initPages(st.Size())
//
//	if w.fileSize < metaSize {
//		return io.ErrUnexpectedEOF
//	}
//	if err := w.readMeta(); err != nil {
//		return err
//	}
//
//	if w.fileSize < metaSize+int64(w.Meta.retentionCount)*retentionSize {
//		return io.ErrUnexpectedEOF
//	}
//	if err := w.readRetentions(); err != nil {
//		return err
//	}
//	w.initBaseIntervals()
//	return nil
//}
//
//func (w *Whisper) Close() error {
//	for _, page := range w.pages {
//		w.bufPool.Put(page)
//	}
//	return w.file.Close()
//}
//
//func (w *Whisper) readMeta() error {
//	//log.Printf("readMeta start, before readPagesIfNeeded")
//	if err := w.readPagesIfNeeded(0, 0); err != nil {
//		return err
//	}
//
//	w.Meta.AggregationMethod = AggregationMethod(w.uint32At(0))
//	w.Meta.maxRetention = Duration(w.uint32At(uint32Size))
//	w.Meta.XFilesFactor = w.float32At(2 * uint32Size)
//	w.Meta.retentionCount = w.uint32At(3 * uint32Size)
//	return nil
//}
//
//func (w *Whisper) readRetentions() error {
//	nRet := int64(w.Meta.retentionCount)
//	off := metaSize + nRet*retentionSize
//	until := int(off / w.pageSize)
//	if err := w.readPagesIfNeeded(0, until); err != nil {
//		return err
//	}
//
//	w.Retentions = make([]Retention, nRet)
//	for i := int64(0); i < nRet; i++ {
//		off := metaSize + i*retentionSize
//		w.Retentions[i] = w.retentionAt(off)
//	}
//	return nil
//}
//
//func (w *Whisper) initBaseIntervals() {
//	w.baseIntervals = make([]Timestamp, len(w.Retentions))
//	for i := range w.baseIntervals {
//		w.baseIntervals[i] = math.MaxUint32
//	}
//}
//
//func (w *Whisper) baseInterval(retentionID int) (Timestamp, error) {
//	interval := w.baseIntervals[retentionID]
//	log.Printf("baseInterval start retentionID=%d, interval=%d", retentionID, interval)
//	if interval != math.MaxUint32 {
//		return interval, nil
//	}
//
//	r := w.Retentions[retentionID]
//	off := int64(r.offset)
//	fromPg := int(off / w.pageSize)
//	untilPg := int((off + uint32Size) / w.pageSize)
//	log.Printf("baseInterval before readPagesIfNeeded, off=%d, fromPg=%d, untilPg=%d", off, fromPg, untilPg)
//	if err := w.readPagesIfNeeded(fromPg, untilPg); err != nil {
//		return 0, fmt.Errorf("cannot read page from %d to %d for baseInterval: %s",
//			fromPg, untilPg, err)
//	}
//	return w.timestampAt(off), nil
//}
//
//func (w *Whisper) readPagesIfNeeded(from, until int) error {
//	log.Printf("readPagesIfNeeded start, from=%d, until=%d", from, until)
//	for from <= until {
//		for from <= until {
//			if _, ok := w.pages[from]; ok {
//				from++
//			} else {
//				break
//			}
//		}
//		if from > until {
//			return nil
//		}
//
//		iovs := [][]byte{w.allocBuf(from)}
//		pid := from + 1
//		for pid <= until {
//			if _, ok := w.pages[pid]; !ok {
//				iovs = append(iovs, w.allocBuf(pid))
//				pid++
//			} else {
//				break
//			}
//		}
//
//		off := int64(from) * w.pageSize
//		log.Printf("readPagesIfNeeded before preadvFull from=%d, to=%d, off=%d", from, pid-1, off)
//		n, err := preadvFull(w.file, iovs, off)
//		log.Printf("readPagesIfNeeded after preadvFull from=%d, to=%d, off=%d, n=%d, err=%v", from, pid-1, off, n, err)
//		if err != nil {
//			return err
//		}
//
//		for ; from < pid; from++ {
//			w.pages[from] = iovs[0]
//			iovs = iovs[1:]
//		}
//	}
//	return nil
//}
//
//func (w *Whisper) allocBuf(pid int) []byte {
//	b := w.bufPool.Get()
//	if pid == w.lastPageID {
//		b = b[:w.lastPageSize]
//	}
//	return b
//}
//
//func (w *Whisper) Flush() error {
//	pgids := make([]int, 0, len(w.dirtyPages))
//	for pgid := range w.dirtyPages {
//		pgids = append(pgids, pgid)
//	}
//	sort.Sort(intSlice(pgids))
//
//	var off int64
//	var iovs [][]byte
//	for i, pgid := range pgids {
//		if i == 0 || pgids[i-1] != pgid-1 {
//			off = int64(pgid) * w.pageSize
//			iovs = [][]byte{w.pages[pgid]}
//		} else {
//			iovs = append(iovs, w.pages[pgid])
//		}
//
//		if i == len(pgids)-1 || pgids[i+1] != pgid+1 {
//			_, err := pwritevFull(w.file, iovs, off)
//			if err != nil {
//				return err
//			}
//		}
//	}
//	if err := w.file.Sync(); err != nil {
//		return err
//	}
//	w.dirtyPages = make(map[int]struct{})
//	return nil
//}
//
//func (w *Whisper) markPageDirty(pgid int) {
//	w.dirtyPages[pgid] = struct{}{}
//}
//
//func (w *Whisper) fillDerivedValuesInHeader() {
//	w.Meta.maxRetention = w.Retentions[len(w.Retentions)-1].MaxRetention()
//	w.Meta.retentionCount = uint32(len(w.Retentions))
//	off := metaSize + len(w.Retentions)*retentionSize
//	for i := range w.Retentions {
//		r := &w.Retentions[i]
//		r.offset = uint32(off)
//		off += int(r.NumberOfPoints) * pointSize
//	}
//}
//
//func (w *Whisper) fileSizeFromHeader() int64 {
//	sz := int64(metaSize)
//	for _, r := range w.Retentions {
//		sz += retentionSize + int64(r.NumberOfPoints)*pointSize
//	}
//	return sz
//}
//
//func (w *Whisper) FetchFromArchive(retentionID int, from, until, now Timestamp) ([]Point, error) {
//	if now == 0 {
//		now = TimestampFromStdTime(time.Now())
//	}
//	log.Printf("FetchFromSpecifiedArchive start, from=%s, until=%s, now=%s", from, until, now)
//	if from > until {
//		return nil, fmt.Errorf("invalid time interval: from time '%d' is after until time '%d'", from, until)
//	}
//	oldest := now.Add(-w.Meta.maxRetention)
//	// range is in the future
//	if from > now {
//		return nil, nil
//	}
//	// range is beyond retention
//	if until < oldest {
//		return nil, nil
//	}
//	if from < oldest {
//		from = oldest
//	}
//	if until > now {
//		until = now
//	}
//	log.Printf("FetchFromSpecifiedArchive adjusted, from=%s, until=%s, now=%s", from, until, now)
//
//	if retentionID < 0 || len(w.Retentions)-1 < retentionID {
//		return nil, ErrRetentionIDOutOfRange
//	}
//
//	baseInterval, err := w.baseInterval(retentionID)
//	log.Printf("FetchFromSpecifiedArchive retentionID=%d, baseInterval=%s, err=%v", retentionID, baseInterval, err)
//	if err != nil {
//		return nil, err
//	}
//
//	r := &w.Retentions[retentionID]
//	fromInterval := r.interval(from)
//	untilInterval := r.interval(until)
//	step := r.SecondsPerPoint
//
//	if baseInterval == 0 {
//		points := make([]Point, (untilInterval-fromInterval)/Timestamp(step))
//		t := fromInterval
//		for i := range points {
//			points[i].Time = t
//			points[i].Value.SetNaN()
//			t = t.Add(step)
//		}
//		return points, nil
//	}
//
//	// Zero-length time range: always include the next point
//	if fromInterval == untilInterval {
//		untilInterval = untilInterval.Add(step)
//	}
//
//	points, err := w.fetchRawPoints(fromInterval, untilInterval, retentionID)
//	if err != nil {
//		return nil, err
//	}
//	log.Printf("FetchFromArchive after fetchRawPoints, retentionID=%d, len(points)=%d", retentionID, len(points))
//	//for i, pt := range points {
//	//	log.Printf("rawPoint i=%d, time=%s, value=%s", i, pt.Time, pt.Value)
//	//}
//	clearOldPoints(points, fromInterval, step)
//	log.Printf("FetchFromArchive after clearOldPoints, retentionID=%d, len(points)=%d", retentionID, len(points))
//
//	return points, nil
//}
//
//func (w *Whisper) fetchRawPoints(fromInterval, untilInterval Timestamp, retentionID int) ([]Point, error) {
//	if err := w.readPagesForIntervalRange(fromInterval, untilInterval, retentionID); err != nil {
//		return nil, err
//	}
//
//	r := &w.Retentions[retentionID]
//	baseInterval, err := w.baseInterval(retentionID)
//	if err != nil {
//		return nil, err
//	}
//
//	fromOffset := r.pointOffsetAt(r.pointIndex(baseInterval, fromInterval))
//	untilOffset := r.pointOffsetAt(r.pointIndex(baseInterval, untilInterval))
//	if fromOffset < untilOffset {
//		points := make([]Point, (untilOffset-fromOffset)/pointSize)
//		offset := fromOffset
//		for i := range points {
//			points[i].Time = w.timestampAt(offset)
//			points[i].Value = w.valueAt(offset + uint32Size)
//			offset += pointSize
//		}
//		return points, nil
//	}
//
//	step := r.SecondsPerPoint
//	points := make([]Point, (untilInterval-fromInterval)/Timestamp(step))
//
//	retentionStartOffset := int64(r.offset)
//	retentionEndOffset := retentionStartOffset + int64(r.NumberOfPoints)*pointSize
//
//	offset := fromOffset
//	i := 0
//	for offset < retentionEndOffset {
//		points[i].Time = w.timestampAt(offset)
//		points[i].Value = w.valueAt(offset + uint32Size)
//		offset += pointSize
//		i++
//	}
//	offset = retentionStartOffset
//	for offset < untilOffset {
//		points[i].Time = w.timestampAt(offset)
//		points[i].Value = w.valueAt(offset + uint32Size)
//		offset += pointSize
//		i++
//	}
//	return points, nil
//}
//
//func (w *Whisper) UpdatePointForArchive(retentionID int, t Timestamp, v Value, now Timestamp) error {
//	points := []Point{{Time: t, Value: v}}
//	return w.UpdatePointsForArchive(retentionID, points, now)
//}
//
//func (w *Whisper) UpdatePointsForArchive(retentionID int, points []Point, now Timestamp) error {
//	if now == 0 {
//		now = TimestampFromStdTime(time.Now())
//	}
//
//	r := &w.Retentions[retentionID]
//	points = r.filterPoints(points, now)
//	if len(points) == 0 {
//		return nil
//	}
//
//	sort.Stable(Points(points))
//	alignedPoints := alignPoints(r, points)
//
//	fromInterval := alignedPoints[0].Time
//	untilInterval := alignedPoints[len(alignedPoints)-1].Time
//	if err := w.readPagesForIntervalRange(fromInterval, untilInterval, retentionID); err != nil {
//		return err
//	}
//
//	baseInterval, err := w.baseInterval(retentionID)
//	if err != nil {
//		return err
//	}
//	if baseInterval == 0 {
//		baseInterval = r.intervalForWrite(now)
//	}
//
//	for _, p := range alignedPoints {
//		offset := r.pointOffsetAt(r.pointIndex(baseInterval, p.Time))
//		w.putTimestampAt(p.Time, offset)
//		w.putValueAt(p.Value, offset+uint32Size)
//	}
//
//	lowRetID := retentionID + 1
//	if lowRetID < len(w.Retentions) {
//		rLow := &w.Retentions[lowRetID]
//		ts := rLow.timesToPropagate(alignedPoints)
//		for ; lowRetID < len(w.Retentions) && len(ts) > 0; lowRetID++ {
//			ts, err = w.propagate(lowRetID, ts, now)
//			if err != nil {
//				return err
//			}
//		}
//	}
//
//	return nil
//}
//
//func (r *Retention) timesToPropagate(points []Point) []Timestamp {
//	var ts []Timestamp
//	for _, p := range points {
//		t := r.intervalForWrite(p.Time)
//		if len(ts) > 0 && t == ts[len(ts)-1] {
//			continue
//		}
//		ts = append(ts, t)
//	}
//	return ts
//}
//
//func (w *Whisper) propagate(retentionID int, ts []Timestamp, now Timestamp) (propagatedTs []Timestamp, err error) {
//	if len(ts) == 0 {
//		return nil, nil
//	}
//	if err := w.readPagesForIntervalRange(ts[0], ts[len(ts)-1], retentionID); err != nil {
//		return nil, err
//	}
//
//	r := &w.Retentions[retentionID]
//
//	baseInterval, err := w.baseInterval(retentionID)
//	if err != nil {
//		return nil, err
//	}
//	if baseInterval == 0 {
//		baseInterval = r.intervalForWrite(now)
//	}
//
//	step := r.SecondsPerPoint
//	highRetID := retentionID - 1
//	var rLow *Retention
//	if retentionID+1 < len(w.Retentions) {
//		rLow = &w.Retentions[retentionID+1]
//	}
//
//	for _, t := range ts {
//		fromInterval := t
//		untilInterval := t.Add(step)
//		points, err := w.fetchRawPoints(fromInterval, untilInterval, highRetID)
//		if err != nil {
//			return nil, err
//		}
//		values := filterValidValues(points, fromInterval, step)
//		knownFactor := float32(len(values)) / float32(len(points))
//		if knownFactor < w.Meta.XFilesFactor {
//			continue
//		}
//
//		v := aggregate(w.Meta.AggregationMethod, values)
//		offset := r.pointOffsetAt(r.pointIndex(baseInterval, t))
//		w.putTimestampAt(t, offset)
//		w.putValueAt(v, offset+uint32Size)
//
//		if rLow != nil {
//			tLow := rLow.intervalForWrite(t)
//			if len(propagatedTs) == 0 || propagatedTs[len(propagatedTs)-1] != tLow {
//				propagatedTs = append(propagatedTs, tLow)
//			}
//		}
//	}
//
//	return propagatedTs, nil
//}
//
//func sum(values []Value) Value {
//	result := Value(0)
//	for _, value := range values {
//		result += value
//	}
//	return result
//}
//
//func aggregate(method AggregationMethod, knownValues []Value) Value {
//	switch method {
//	case Average:
//		return sum(knownValues) / Value(len(knownValues))
//	case Sum:
//		return sum(knownValues)
//	case First:
//		return knownValues[0]
//	case Last:
//		return knownValues[len(knownValues)-1]
//	case Max:
//		max := knownValues[0]
//		for _, value := range knownValues {
//			if value > max {
//				max = value
//			}
//		}
//		return max
//	case Min:
//		min := knownValues[0]
//		for _, value := range knownValues {
//			if value < min {
//				min = value
//			}
//		}
//		return min
//	}
//	panic("Invalid aggregation method")
//}
//
//func (w *Whisper) readPagesForIntervalRange(fromInterval, untilInterval Timestamp, retentionID int) error {
//	r := &w.Retentions[retentionID]
//	baseInterval, err := w.baseInterval(retentionID)
//	if err != nil {
//		return err
//	}
//
//	fromOffset := r.pointOffsetAt(r.pointIndex(baseInterval, fromInterval))
//	untilOffset := r.pointOffsetAt(r.pointIndex(baseInterval, untilInterval))
//	fromPg := w.intForStartOffset(fromOffset)
//	untilPg := w.intForEndOffset(untilOffset + pointSize)
//	if fromPg <= untilPg {
//		if err := w.readPagesIfNeeded(fromPg, untilPg); err != nil {
//			return fmt.Errorf("cannot read page from %d to %d for points: %s",
//				fromPg, untilPg, err)
//		}
//	}
//
//	retentionStartOffset := int64(r.offset)
//	retentionEndOffset := retentionStartOffset + int64(r.NumberOfPoints)*pointSize
//	retentionStartPg := w.intForStartOffset(retentionStartOffset)
//	retentionEndPg := w.intForEndOffset(retentionEndOffset)
//
//	if untilPg+1 < fromPg {
//		if err := w.readPagesIfNeeded(retentionStartPg, untilPg); err != nil {
//			return fmt.Errorf("cannot read page from %d to %d for points: %s",
//				retentionStartPg, untilPg, err)
//		}
//		if err := w.readPagesIfNeeded(fromPg, retentionEndPg); err != nil {
//			return fmt.Errorf("cannot read page from %d to %d for points: %s",
//				fromPg, retentionEndPg, err)
//		}
//	} else {
//		if err := w.readPagesIfNeeded(retentionStartPg, retentionEndPg); err != nil {
//			return fmt.Errorf("cannot read page from %d to %d for points: %s",
//				retentionStartPg, retentionEndPg, err)
//		}
//	}
//	return nil
//}
//
//func clearOldPoints(points []Point, fromInterval Timestamp, step Duration) {
//	currentInterval := fromInterval
//	for i := range points {
//		if points[i].Time != currentInterval {
//			points[i].Time = currentInterval
//			points[i].Value.SetNaN()
//		}
//		currentInterval = currentInterval.Add(step)
//	}
//}
//
//func filterValidValues(points []Point, fromInterval Timestamp, step Duration) []Value {
//	values := make([]Value, 0, len(points))
//	currentInterval := fromInterval
//	for _, p := range points {
//		if p.Time != currentInterval {
//			continue
//		}
//		values = append(values, p.Value)
//		currentInterval = currentInterval.Add(step)
//	}
//	return values
//}
//
//func (w *Whisper) GetRawPoints(retentionID int) []Point {
//	r := w.Retentions[retentionID]
//	off := int64(r.offset)
//	p := make([]Point, r.NumberOfPoints)
//	for i := uint32(0); i < r.NumberOfPoints; i++ {
//		p[i].Time = w.timestampAt(off)
//		p[i].Value = w.valueAt(off + uint32Size)
//		off += pointSize
//	}
//	return p
//}
//
//func (w *Whisper) uint32At(offset int64) uint32 {
//	buf := w.bufForValue(offset, uint32Size)
//	return binary.BigEndian.Uint32(buf)
//}
//
//func (w *Whisper) uint64At(offset int64) uint64 {
//	buf := w.bufForValue(offset, uint64Size)
//	return binary.BigEndian.Uint64(buf)
//}
//
//func (w *Whisper) float32At(offset int64) float32 {
//	return math.Float32frombits(w.uint32At(offset))
//}
//
//func (w *Whisper) float64At(offset int64) float64 {
//	return math.Float64frombits(w.uint64At(offset))
//}
//
//func (w *Whisper) valueAt(offset int64) Value {
//	return Value(w.float64At(offset))
//}
//
//func (w *Whisper) bufForValue(offset, size int64) []byte {
//	pgID := int(offset / w.pageSize)
//	offInPg := offset % w.pageSize
//	overflow := offInPg + size - w.pageSize
//	if overflow <= 0 {
//		p := w.pages[pgID]
//		return p[offInPg : offInPg+size]
//	}
//
//	buf := make([]byte, size)
//	p0 := w.pages[pgID]
//	p1 := w.pages[pgID+1]
//	copy(buf, p0[offInPg:])
//	copy(buf[size-overflow:], p1[:overflow])
//	return buf
//}
//
//func (w *Whisper) markPagesDirty(offset, size int64) {
//	startPgID := int(offset / w.pageSize)
//	endPgID := int((offset + size) / w.pageSize)
//	endOffInPg := (offset + size) % w.pageSize
//	if endOffInPg == 0 {
//		endPgID--
//	}
//
//	for i := startPgID; i <= endPgID; i++ {
//		w.dirtyPages[i] = struct{}{}
//	}
//}
//
//func (w *Whisper) retentionAt(offset int64) Retention {
//	return Retention{
//		offset:          w.uint32At(offset),
//		SecondsPerPoint: Duration(w.uint32At(offset + uint32Size)),
//		NumberOfPoints:  w.uint32At(offset + 2*uint32Size),
//	}
//}
//
//func (w *Whisper) timestampAt(offset int64) Timestamp {
//	return Timestamp(w.uint32At(offset))
//}
//
//func (w *Whisper) putUint32At(v uint32, offset int64) {
//	startPg := w.intForStartOffset(offset)
//	endPg := w.intForEndOffset(offset + uint32Size)
//	startOffInPg := w.offsetInPageForStartOffset(offset)
//	if startPg == endPg {
//		binary.BigEndian.PutUint32(w.pages[startPg][startOffInPg:], v)
//		w.markPageDirty(startPg)
//		return
//	}
//	var buf [uint32Size]byte
//	binary.BigEndian.PutUint32(buf[:], v)
//	endOffInPg := w.offsetInPageForEndOffset(offset + uint32Size)
//	w.splitCopyToPages(startPg, startOffInPg, endPg, endOffInPg, buf[:])
//}
//
//func (w *Whisper) putUint64At(v uint64, offset int64) {
//	startPg := w.intForStartOffset(offset)
//	endPg := w.intForEndOffset(offset + uint64Size)
//	startOffInPg := w.offsetInPageForStartOffset(offset)
//	if startPg == endPg {
//		binary.BigEndian.PutUint64(w.pages[startPg][startOffInPg:], v)
//		w.markPageDirty(startPg)
//		return
//	}
//	var buf [uint64Size]byte
//	binary.BigEndian.PutUint64(buf[:], v)
//	endOffInPg := w.offsetInPageForEndOffset(offset + uint64Size)
//	w.splitCopyToPages(startPg, startOffInPg, endPg, endOffInPg, buf[:])
//}
//
//func (w *Whisper) putFloat32At(v float32, offset int64) {
//	w.putUint32At(math.Float32bits(v), offset)
//}
//
//func (w *Whisper) putFloat64At(v float64, offset int64) {
//	w.putUint64At(math.Float64bits(v), offset)
//}
//
//func (w *Whisper) putTimestampAt(t Timestamp, offset int64) {
//	w.putUint32At(uint32(t), offset)
//}
//
//func (w *Whisper) putValueAt(v Value, offset int64) {
//	w.putFloat64At(float64(v), offset)
//}
//
//func (w *Whisper) putMeta() {
//	w.putUint32At(uint32(w.Meta.AggregationMethod), 0)
//	w.putUint32At(uint32(w.Meta.maxRetention), uint32Size)
//	w.putFloat32At(w.Meta.XFilesFactor, 2*uint32Size)
//	w.putUint32At(uint32(w.Meta.retentionCount), 3*uint32Size)
//}
//
//func (w *Whisper) putRetentions() {
//	off := int64(metaSize)
//	for i := range w.Retentions {
//		r := &w.Retentions[i]
//		w.putUint32At(r.offset, off)
//		w.putUint32At(uint32(r.SecondsPerPoint), off+uint32Size)
//		w.putUint32At(r.NumberOfPoints, off+2*uint32Size)
//		off += retentionSize
//	}
//}
//
//func (w *Whisper) splitCopyToPages(startPg int, startOffInPg int64, endPg int, endOffInPg int64, b []byte) {
//	sz := w.pageSize - startOffInPg
//	copy(w.pages[startPg][startOffInPg:], b[:sz])
//	copy(w.pages[endPg][:endOffInPg], b[sz:sz+endOffInPg])
//	w.markPageDirty(startPg)
//	w.markPageDirty(endPg)
//}
//
//func (w *Whisper) intForStartOffset(offset int64) int {
//	return int(offset / w.pageSize)
//}
//
//func (w *Whisper) offsetInPageForStartOffset(offset int64) int64 {
//	return offset % w.pageSize
//}
//
//func (w *Whisper) intForEndOffset(offset int64) int {
//	pgID := int(offset / w.pageSize)
//	if offset%w.pageSize == 0 && pgID > 0 {
//		pgID--
//	}
//	return pgID
//}
//
//func (w *Whisper) offsetInPageForEndOffset(offset int64) int64 {
//	offInPg := offset % w.pageSize
//	if offInPg == 0 && offset > 0 {
//		offInPg += w.pageSize
//	}
//	return offInPg
//}

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

func (pp Points) Len() int           { return len(pp) }
func (pp Points) Less(i, j int) bool { return pp[i].Time < pp[j].Time }
func (pp Points) Swap(i, j int)      { pp[i], pp[j] = pp[j], pp[i] }
