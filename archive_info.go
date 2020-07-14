package whispertool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

// ArchiveInfo is a retention level.
// ArchiveInfo levels describe a given archive in the database. How detailed it is and how far back it records.
type ArchiveInfo struct {
	offset          uint32
	secondsPerPoint Duration
	numberOfPoints  uint32
}

// ArchiveInfoList is a slice of Retention.
type ArchiveInfoList []ArchiveInfo

// ArchiveIDBest is used to find the best archive for time range in FetchFromArchive.
const ArchiveIDBest = -1

// ErrArchiveIDOutOfRange is the error when an archive ID if out of range.
var ErrArchiveIDOutOfRange = errors.New("archive ID out of range")

// NewArchiveInfo creats a retention.
func NewArchiveInfo(secondsPerPoint Duration, numberOfPoints uint32) ArchiveInfo {
	return ArchiveInfo{
		secondsPerPoint: secondsPerPoint,
		numberOfPoints:  numberOfPoints,
	}
}

func (a *ArchiveInfo) timesToPropagate(points []Point) []Timestamp {
	var ts []Timestamp
	for _, p := range points {
		t := a.intervalForWrite(p.Time)
		if len(ts) > 0 && t == ts[len(ts)-1] {
			continue
		}
		ts = append(ts, t)
	}
	return ts
}

// ParseArchiveInfoList parses multiple retention definitions as you would find in the storage-schemas.conf
// of a Carbon install. Note that this parses multiple retention definitions.
// An example input is "10s:2h,1m:1d".
//
// See: http://graphite.readthedocs.org/en/1.0/config-carbon.html#storage-schemas-conf
func ParseArchiveInfoList(s string) (ArchiveInfoList, error) {
	if len(s) == 0 {
		return nil, fmt.Errorf("invalid ArchiveInfoList: %q", s)
	}
	var archiveInfoList ArchiveInfoList
	for {
		var rStr string
		i := strings.IndexRune(s, ',')
		if i == -1 {
			rStr = s
		} else {
			rStr = s[:i]
		}
		archiveInfo, err := ParseArchiveInfo(rStr)
		if err != nil {
			return nil, fmt.Errorf("invalid ArchiveInfoList: %q", s)
		}
		archiveInfoList = append(archiveInfoList, archiveInfo)

		if i == -1 {
			break
		}
		if i+1 >= len(s) {
			return nil, fmt.Errorf("invalid ArchiveInfoList: %q", s)
		}
		s = s[i+1:]
	}

	archiveInfoList.fillOffset()
	if err := archiveInfoList.validate(); err != nil {
		return nil, err
	}
	return archiveInfoList, nil
}

// ParseArchiveInfo parses a single retention definition as you would find in the storage-schemas.conf
// of a Carbon install. Note that this only parses a single retention definition.
// An example input is "10s:2h".
// If you would like to parse multiple retention definitions like "10s:2h,1m:1d", use
// ParseRetentions instead.
func ParseArchiveInfo(s string) (ArchiveInfo, error) {
	i := strings.IndexRune(s, ':')
	if i == -1 || i+1 >= len(s) {
		return ArchiveInfo{}, fmt.Errorf("invalid ArchiveInfo: %q", s)
	}

	step, err := ParseDuration(s[:i])
	if err != nil {
		return ArchiveInfo{}, fmt.Errorf("invalid ArchiveInfo: %q", s)
	}
	d, err := ParseDuration(s[i+1:])
	if err != nil {
		return ArchiveInfo{}, fmt.Errorf("invalid ArchiveInfo: %q", s)
	}
	if step <= 0 || d <= 0 || d%step != 0 {
		return ArchiveInfo{}, fmt.Errorf("invalid ArchiveInfo: %q", s)
	}
	return ArchiveInfo{
		secondsPerPoint: step,
		numberOfPoints:  uint32(d / step),
	}, nil
}

// String returns the spring representation of rr.
func (aa ArchiveInfoList) String() string {
	var b strings.Builder
	for i, r := range aa {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(r.String())
	}
	return b.String()
}

// Equal returns whether or not aa equals to bb.
func (aa ArchiveInfoList) Equal(bb ArchiveInfoList) bool {
	if len(aa) != len(bb) {
		return false
	}
	for i, r := range aa {
		if !r.Equal(bb[i]) {
			return false
		}
	}
	return true
}

func (aa ArchiveInfoList) fillOffset() {
	off := metaSize + uint32(len(aa))*archiveInfoListSize
	for i := range aa {
		a := &aa[i]
		a.offset = off
		off += uint32(a.numberOfPoints) * pointSize
	}
}

func (aa ArchiveInfoList) validate() error {
	if len(aa) == 0 {
		return fmt.Errorf("no retentions")
	}

	off := metaSize + uint32(len(aa))*archiveInfoListSize
	for i, a := range aa {
		if err := a.validate(); err != nil {
			return fmt.Errorf("invalid archive%v: %v", i, err)
		}
		if a.offset != off {
			return fmt.Errorf("invalid archive%v: invalid offset got:%v, want:%v", i, a.offset, off)
		}

		if i == len(aa)-1 {
			break
		}

		rNext := aa[i+1]
		if !(a.secondsPerPoint < rNext.secondsPerPoint) {
			return fmt.Errorf("a Whisper database may not be configured having two archives with the same precision (archive%v: %v, archive%v: %v)", i, a, i+1, rNext)
		}

		if rNext.secondsPerPoint%a.secondsPerPoint != 0 {
			return fmt.Errorf("higher precision archives' precision must evenly divide all lower precision archives' precision (archive%v: %v, archive%v: %v)", i, a.secondsPerPoint, i+1, rNext.secondsPerPoint)
		}

		if a.MaxRetention() >= rNext.MaxRetention() {
			return fmt.Errorf("lower precision archives must cover larger time intervals than higher precision archives (archive%v: %v seconds, archive%v: %v seconds)", i, a.MaxRetention(), i+1, rNext.MaxRetention())
		}

		if a.numberOfPoints < uint32(rNext.secondsPerPoint/a.secondsPerPoint) {
			return fmt.Errorf("each archive must have at least enough points to consolidate to the next archive (archive%v consolidates %v of archive%v's points but it has only %v total points)", i+1, rNext.secondsPerPoint/a.secondsPerPoint, i, a.numberOfPoints)
		}

		off += uint32(a.numberOfPoints) * pointSize
	}
	return nil
}

type archiveInfoListByPrecision ArchiveInfoList

func (a archiveInfoListByPrecision) Len() int {
	return len(a)
}

func (a archiveInfoListByPrecision) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a archiveInfoListByPrecision) Less(i, j int) bool {
	return a[i].secondsPerPoint < a[j].secondsPerPoint
}

// SecondsPerPoint returns the duration of the step of a.
func (a *ArchiveInfo) SecondsPerPoint() Duration { return a.secondsPerPoint }

// NumberOfPoints returns the number of points in a.
func (a *ArchiveInfo) NumberOfPoints() uint32 { return a.numberOfPoints }

func (a ArchiveInfo) validate() error {
	if a.secondsPerPoint <= 0 {
		return errors.New("seconds per point must be positive")
	}
	if a.numberOfPoints <= 0 {
		return errors.New("number of points must be positive")
	}
	return nil
}

// Equal returns whether or not a equals to b.
func (a ArchiveInfo) Equal(b ArchiveInfo) bool {
	return a.secondsPerPoint == b.secondsPerPoint &&
		a.numberOfPoints == b.numberOfPoints
}

// String returns the spring representation of a.
func (a ArchiveInfo) String() string {
	return a.secondsPerPoint.String() + ":" +
		(a.secondsPerPoint * Duration(a.numberOfPoints)).String()
}

func (a *ArchiveInfo) pointIndex(baseInterval, interval Timestamp) int {
	// NOTE: We use interval.Sub(baseInterval) here instead of
	// interval - baseInterval since the latter produces
	// wrong values because of underflow when interval < baseInterval.
	// Another solution would be (int64(interval) - int64(baseInterval))
	pointDistance := int64(interval.Sub(baseInterval)) / int64(a.secondsPerPoint)
	return int(floorMod(pointDistance, int64(a.numberOfPoints)))
}

// MaxRetention returns the whole duration of a.
func (a *ArchiveInfo) MaxRetention() Duration {
	return a.secondsPerPoint * Duration(a.numberOfPoints)
}

func (a *ArchiveInfo) pointOffsetAt(index int) uint32 {
	return a.offset + uint32(index)*pointSize
}

// interval returns the aligned interval of t to a for fetching data points.
func (a *ArchiveInfo) interval(t Timestamp) Timestamp {
	step := int64(a.secondsPerPoint)
	return Timestamp(int64(t) - floorMod(int64(t), step) + step)
}

func (a *ArchiveInfo) intervalForWrite(t Timestamp) Timestamp {
	step := int64(a.secondsPerPoint)
	return Timestamp(int64(t) - floorMod(int64(t), step))
}

func (a *ArchiveInfo) filterPoints(points []Point, now Timestamp) []Point {
	oldest := a.intervalForWrite(now.Add(-a.MaxRetention()))
	filteredPoints := make([]Point, 0, len(points))
	for _, p := range points {
		if p.Time >= oldest && p.Time <= now {
			filteredPoints = append(filteredPoints, p)
		}
	}
	return filteredPoints
}

// alignPoints returns a new slice of Point whose time is aligned
// with calling intervalForWrite method.
// Note that the input points must be sorted by Time in advance.
func (a *ArchiveInfo) alignPoints(points []Point) []Point {
	alignedPoints := make([]Point, 0, len(points))
	var prevTime Timestamp
	for i, point := range points {
		dPoint := Point{
			Time:  a.intervalForWrite(point.Time),
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

// AppendTo appends encoded bytes of a to dst
// and returns the extended buffer.
//
// AppendTo method implements the AppenderTo interface.
func (a *ArchiveInfo) AppendTo(dst []byte) []byte {
	var b [uint32Size]byte

	binary.BigEndian.PutUint32(b[:], uint32(a.offset))
	dst = append(dst, b[:]...)

	dst = a.secondsPerPoint.AppendTo(dst)

	binary.BigEndian.PutUint32(b[:], uint32(a.numberOfPoints))
	return append(dst, b[:]...)
}

// TakeFrom updates a from encoded bytes in src
// and returns the rest of src.
//
// TakeFrom method implements the TakerFrom interface.
// If there is an error, it may be of type *WantLargerBufferError.
func (a *ArchiveInfo) TakeFrom(src []byte) ([]byte, error) {
	if len(src) < archiveInfoListSize {
		return nil, &WantLargerBufferError{WantedBufSize: archiveInfoListSize}
	}

	a.offset = binary.BigEndian.Uint32(src)
	src = src[uint32Size:]

	src, err := a.secondsPerPoint.TakeFrom(src)
	if err != nil {
		return nil, err
	}

	a.numberOfPoints = binary.BigEndian.Uint32(src)
	return src[uint32Size:], nil
}
