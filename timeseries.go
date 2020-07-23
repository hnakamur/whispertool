package whispertool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// TimeSeries is a result from Fetch.
type TimeSeries struct {
	fromTime  Timestamp
	untilTime Timestamp
	step      Duration
	values    []Value
}

// Points represents a slice of Point.
type Points []Point

// Point represent a data point in whisper databases.
type Point struct {
	Time  Timestamp
	Value Value
}

// Value represents a value of Point.
type Value float64

// NewTimeSeries returns a new TimeSeries.
func NewTimeSeries(fromTime, untilTime Timestamp, step Duration, values []Value) *TimeSeries {
	return &TimeSeries{
		fromTime:  fromTime,
		untilTime: untilTime,
		step:      step,
		values:    values,
	}
}

// FromTime returns the start time of ts.
func (ts *TimeSeries) FromTime() Timestamp { return ts.fromTime }

// UntilTime returns the end time of ts.
func (ts *TimeSeries) UntilTime() Timestamp { return ts.untilTime }

// Step returns the duration between points in ts.
func (ts *TimeSeries) Step() Duration { return ts.step }

// Points converts ts to points.
func (ts *TimeSeries) Points() Points {
	pts := make([]Point, len(ts.values))
	for i, v := range ts.values {
		pts[i] = Point{
			Time:  ts.fromTime.Add(Duration(i) * ts.step),
			Value: v,
		}
	}
	return pts
}

// EqualTimeRangeAndStep returns whether or not all of
// FromTime(), UntilTime() and Step() are the same
// between ts and us.
func (ts *TimeSeries) EqualTimeRangeAndStep(ts2 *TimeSeries) bool {
	return ts.FromTime() == ts2.FromTime() &&
		ts.UntilTime() == ts2.UntilTime() &&
		ts.Step() == ts2.Step()
}

// Equal returns whether or not ts equals to us.
func (ts *TimeSeries) Equal(ts2 *TimeSeries) bool {
	return ts.EqualTimeRangeAndStep(ts2) &&
		ts.valuesEqual(ts2)
}

func (ts *TimeSeries) valuesEqual(ts2 *TimeSeries) bool {
	if len(ts.Values()) != len(ts2.Values()) {
		return false
	}
	for i, v := range ts.Values() {
		v2 := ts2.Values()[i]
		if !v.Equal(v2) {
			return false
		}
	}
	return true
}

// DiffPoints returns the different points between ts and us.
func (ts *TimeSeries) DiffPoints(ts2 *TimeSeries) (Points, Points) {
	if len(ts.Values()) != len(ts2.Values()) {
		return ts.Points(), ts2.Points()
	}

	var pts, pts2 Points
	for i, v := range ts.Values() {
		t := ts.FromTime().Add(Duration(i) * ts.Step())
		t2 := ts2.FromTime().Add(Duration(i) * ts.Step())
		v2 := ts2.Values()[i]
		if t != t2 || !v.Equal(v2) {
			pts = append(pts, Point{Time: t, Value: v})
			pts2 = append(pts2, Point{Time: t2, Value: v2})
		}
	}
	return pts, pts2
}

// Values returns the values in ts.
func (ts *TimeSeries) Values() []Value { return ts.values }

// String returns the string representation of ts.
func (ts *TimeSeries) String() string {
	if ts == nil {
		return ""
	}
	return fmt.Sprintf("&{fromTime:%s untilTime:%s step:%s values:%v}",
		ts.fromTime, ts.untilTime, ts.step, ts.values)
}

// AppendTo appends encoded bytes of ts to dst
// and returns the extended buffer.
//
// AppendTo method implements the AppenderTo interface.
func (ts *TimeSeries) AppendTo(dst []byte) []byte {
	dst = ts.fromTime.AppendTo(dst)
	dst = ts.untilTime.AppendTo(dst)
	dst = ts.step.AppendTo(dst)
	values := ts.Values()
	for i := range values {
		dst = values[i].AppendTo(dst)
	}
	return dst
}

// TakeFrom updates ts from encoded bytes in src
// and returns the rest of src.
//
// TakeFrom method implements the TakerFrom interface.
// If there is an error, it may be of type *WantLargerBufferError.
func (ts *TimeSeries) TakeFrom(src []byte) ([]byte, error) {
	if len(src) < 3*uint32Size {
		return nil, &WantLargerBufferError{WantedBufSize: 3 * uint32Size}
	}

	src, err := ts.fromTime.TakeFrom(src)
	if err != nil {
		return nil, err
	}
	src, err = ts.untilTime.TakeFrom(src)
	if err != nil {
		return nil, err
	}
	src, err = ts.step.TakeFrom(src)
	if err != nil {
		return nil, err
	}

	if ts.step == 0 {
		return nil, errors.New("step must not be zero")
	}
	if ts.untilTime < ts.fromTime {
		return nil, errors.New("untilTime is older than fromTime")
	}

	n := int(ts.untilTime.Sub(ts.fromTime) / ts.step)
	wantedSize := n * float64Size
	if len(src) < wantedSize {
		return nil, &WantLargerBufferError{WantedBufSize: 3*uint32Size + wantedSize}
	}

	ts.values = make([]Value, n)
	for i := 0; i < n; i++ {
		src, err = ts.values[i].TakeFrom(src)
		if err != nil {
			return nil, err
		}
	}
	return src, nil
}

// Len is the number of elements in the collection.
// Implements sort.Interface.
func (pp Points) Len() int { return len(pp) }

// Less reports whether the element with
// index i should sort before the element with index j.
// Implements sort.Interface.
func (pp Points) Less(i, j int) bool { return pp[i].Time < pp[j].Time }

// Swap swaps the elements with indexes i and j.
// Implements sort.Interface.
func (pp Points) Swap(i, j int) { pp[i], pp[j] = pp[j], pp[i] }

// Equals returns whether or not pp equals to qq.
func (pp Points) Equal(qq Points) bool {
	if len(pp) != len(qq) {
		return false
	}

	for i, p := range pp {
		q := qq[i]
		if !p.Equal(q) {
			return false
		}
	}
	return true
}

// Diff returns the different points in comparison of pp and qq.
func (pp Points) Diff(qq Points) (Points, Points) {
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

// Values returns values of pp.
func (pp Points) Values() []Value {
	values := make([]Value, len(pp))
	for i, p := range pp {
		values[i] = p.Value
	}
	return values
}

// AppendTo appends encoded bytes of pp to dst
// and returns the extended buffer.
//
// AppendTo method implements the AppenderTo interface.
func (pp *Points) AppendTo(dst []byte) []byte {
	var b [uint64Size]byte
	binary.BigEndian.PutUint64(b[:], uint64(len(*pp)))
	dst = append(dst, b[:]...)
	for i := range *pp {
		dst = (*pp)[i].AppendTo(dst)
	}
	return dst
}

// TakeFrom updates pp from encoded bytes in src
// and returns the rest of src.
//
// TakeFrom method implements the TakerFrom interface.
// If there is an error, it may be of type *WantLargerBufferError.
func (pp *Points) TakeFrom(src []byte) ([]byte, error) {
	if len(src) < uint64Size {
		return nil, &WantLargerBufferError{WantedBufSize: uint64Size}
	}

	count := int(binary.BigEndian.Uint64(src))
	src = src[uint64Size:]

	wantedSize := count * pointSize
	if len(src) < wantedSize {
		return nil, &WantLargerBufferError{WantedBufSize: uint64Size + wantedSize}
	}

	*pp = make(Points, count)
	for i := 0; i < count; i++ {
		var err error
		src, err = (*pp)[i].TakeFrom(src)
		if err != nil {
			return nil, err
		}
	}
	return src, nil
}

// String returns a string representation of pp for debugging.
func (pp Points) String() string {
	var b strings.Builder
	b.WriteString("Points{")
	for i, p := range pp {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(p.String())
	}
	b.WriteString("}")
	return b.String()
}

// String returns a string representation of p for debugging.
func (p Point) String() string {
	return "{" + p.Time.String() + " " + p.Value.String() + "}"
}

// Equals returns whether or not p equals to q.
// It returns true if time and value of p equals to q.
// For comparison of value, Value's Equals method is used.
func (p Point) Equal(q Point) bool {
	return p.Time == q.Time && p.Value.Equal(q.Value)
}

// AppendTo appends encoded bytes of p to dst
// and returns the extended buffer.
//
// AppendTo method implements the AppenderTo interface.
func (p *Point) AppendTo(dst []byte) []byte {
	dst = p.Time.AppendTo(dst)
	return p.Value.AppendTo(dst)
}

// TakeFrom updates p from encoded bytes in src
// and returns the rest of src.
//
// TakeFrom method implements the TakerFrom interface.
// If there is an error, it may be of type *WantLargerBufferError.
func (p *Point) TakeFrom(src []byte) ([]byte, error) {
	if len(src) < pointSize {
		return nil, &WantLargerBufferError{WantedBufSize: pointSize}
	}

	src, err := p.Time.TakeFrom(src)
	if err != nil {
		return nil, err
	}
	src, err = p.Value.TakeFrom(src)
	if err != nil {
		return nil, err
	}
	return src, nil
}

// SetNaN sets the value to NaN.
func (v *Value) SetNaN() {
	*v = Value(math.NaN())
}

// IsNaN returns whether or not v is NaN.
func (v Value) IsNaN() bool {
	return math.IsNaN(float64(v))
}

// String returns the string representation of v.
func (v Value) String() string {
	return strconv.FormatFloat(float64(v), 'f', -1, 64)
}

// Add returns the sum (v + u) if both v and u is not NaN.
// It returns u if v is NaN, v if u is NaN.
func (v Value) Add(u Value) Value {
	if v.IsNaN() {
		return u
	}
	if u.IsNaN() {
		return v
	}
	return v + u
}

// Diff returns the difference (v - u) if both v and u is not NaN.
// It returns NaN if either v or u is NaN.
func (v Value) Diff(u Value) Value {
	if v.IsNaN() || u.IsNaN() {
		return Value(math.NaN())
	}
	return v - u
}

// Equal returns whether or not v equals to u.
// It returns true if both of v and u is NaN,
// or both of v and u is not NaN and v == u.
func (v Value) Equal(u Value) bool {
	pIsNaN := v.IsNaN()
	qIsNaN := u.IsNaN()
	return (pIsNaN && qIsNaN) || (!pIsNaN && !qIsNaN && v == u)
}

// AppendTo appends encoded bytes of v to dst
// and returns the extended buffer.
//
// AppendTo method implements the AppenderTo interface.
func (v *Value) AppendTo(dst []byte) []byte {
	var b [uint64Size]byte
	binary.BigEndian.PutUint64(b[:], math.Float64bits(float64(*v)))
	return append(dst, b[:]...)
}

// TakeFrom updates v from encoded bytes in src
// and returns the rest of src.
//
// TakeFrom method implements the TakerFrom interface.
// If there is an error, it may be of type *WantLargerBufferError.
func (v *Value) TakeFrom(src []byte) ([]byte, error) {
	if len(src) < uint64Size {
		return nil, &WantLargerBufferError{WantedBufSize: uint64Size}
	}
	*v = Value(math.Float64frombits(binary.BigEndian.Uint64(src)))
	return src[uint64Size:], nil
}
