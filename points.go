package whispertool

import (
	"math"
	"strconv"
)

type Value float64

type Point struct {
	Time  Timestamp
	Value Value
}

type Points []Point

func (v *Value) SetNaN() {
	*v = Value(math.NaN())
}

func (v Value) IsNaN() bool {
	return math.IsNaN(float64(v))
}

func (v Value) String() string {
	return strconv.FormatFloat(float64(v), 'f', -1, 64)
}

func (v Value) Add(u Value) Value {
	if v.IsNaN() {
		return u
	}
	if u.IsNaN() {
		return v
	}
	return v + u
}

func (v Value) Diff(u Value) Value {
	if v.IsNaN() || u.IsNaN() {
		return Value(math.NaN())
	}
	return v - u
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
