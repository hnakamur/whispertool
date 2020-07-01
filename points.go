package whispertool

import (
	"math"
	"strconv"
)

// Value represents a value of Point.
type Value float64

// Point represent a data point in whisper databases.
type Point struct {
	Time  Timestamp
	Value Value
}

// Points represents a slice of Point.
type Points []Point

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

// Equals returns whether or not p equals to q.
// It returns true if time and value of p equals to q.
// For comparison of value, Value's Equals method is used.
func (p Point) Equal(q Point) bool {
	return p.Time == q.Time && p.Value.Equal(q.Value)
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
