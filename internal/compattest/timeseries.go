package compattest

import "time"

type Point struct {
	Time  time.Time
	Value float64
}

type TimeSeries struct {
	from   time.Time
	until  time.Time
	step   time.Duration
	values []float64
}

func (ts *TimeSeries) From() time.Time     { return ts.from }
func (ts *TimeSeries) Until() time.Time    { return ts.until }
func (ts *TimeSeries) Step() time.Duration { return ts.step }
func (ts *TimeSeries) Values() []float64   { return ts.values }

func (ts *TimeSeries) Points() []Point {
	if ts.values == nil {
		return nil
	}
	points := make([]Point, len(ts.values))
	for i, v := range ts.values {
		points[i] = Point{
			Time:  ts.From().Add(time.Duration(i) * ts.Step()),
			Value: v,
		}
	}
	return points
}
