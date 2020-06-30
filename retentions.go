package whispertool

import (
	"errors"
	"fmt"
	"strings"
)

type Retention struct {
	offset          uint32
	secondsPerPoint Duration
	numberOfPoints  uint32
}

type Retentions []Retention

func (r *Retention) timesToPropagate(points []Point) []Timestamp {
	var ts []Timestamp
	for _, p := range points {
		t := r.intervalForWrite(p.Time)
		if len(ts) > 0 && t == ts[len(ts)-1] {
			continue
		}
		ts = append(ts, t)
	}
	return ts
}

func ParseRetentions(s string) (Retentions, error) {
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
		rr = append(rr, r)

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

func ParseRetention(s string) (Retention, error) {
	i := strings.IndexRune(s, ':')
	if i == -1 || i+1 >= len(s) {
		return Retention{}, fmt.Errorf("invalid retention: %q", s)
	}

	step, err := ParseDuration(s[:i])
	if err != nil {
		return Retention{}, fmt.Errorf("invalid retention: %q", s)
	}
	d, err := ParseDuration(s[i+1:])
	if err != nil {
		return Retention{}, fmt.Errorf("invalid retention: %q", s)
	}
	if step <= 0 || d <= 0 || d%step != 0 {
		return Retention{}, fmt.Errorf("invalid retention: %q", s)
	}
	return Retention{
		secondsPerPoint: step,
		numberOfPoints:  uint32(d / step),
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
		if !(r.secondsPerPoint < rNext.secondsPerPoint) {
			return fmt.Errorf("a Whisper database may not be configured having two archives with the same precision (archive%v: %v, archive%v: %v)", i, r, i+1, rNext)
		}

		if rNext.secondsPerPoint%r.secondsPerPoint != 0 {
			return fmt.Errorf("higher precision archives' precision must evenly divide all lower precision archives' precision (archive%v: %v, archive%v: %v)", i, r.secondsPerPoint, i+1, rNext.secondsPerPoint)
		}

		if r.MaxRetention() >= rNext.MaxRetention() {
			return fmt.Errorf("lower precision archives must cover larger time intervals than higher precision archives (archive%v: %v seconds, archive%v: %v seconds)", i, r.MaxRetention(), i+1, rNext.MaxRetention())
		}

		if r.numberOfPoints < uint32(rNext.secondsPerPoint/r.secondsPerPoint) {
			return fmt.Errorf("each archive must have at least enough points to consolidate to the next archive (archive%v consolidates %v of archive%v's points but it has only %v total points)", i+1, rNext.secondsPerPoint/r.secondsPerPoint, i, r.numberOfPoints)
		}
	}
	return nil
}

func (r *Retention) SecondsPerPoint() Duration { return r.secondsPerPoint }
func (r *Retention) NumberOfPoints() uint32    { return r.numberOfPoints }

func (r Retention) validate() error {
	if r.secondsPerPoint <= 0 {
		return errors.New("seconds per point must be positive")
	}
	if r.numberOfPoints <= 0 {
		return errors.New("number of points must be positive")
	}
	return nil
}

func (rr Retentions) Equal(ss Retentions) bool {
	if len(rr) != len(ss) {
		return false
	}
	for i, r := range rr {
		if !r.Equal(ss[i]) {
			return false
		}
	}
	return true
}

func (r Retention) Equal(s Retention) bool {
	return r.secondsPerPoint == s.secondsPerPoint &&
		r.numberOfPoints == s.numberOfPoints
}

func (r Retention) String() string {
	return r.secondsPerPoint.String() + ":" +
		(r.secondsPerPoint * Duration(r.numberOfPoints)).String()
}

func (r *Retention) pointIndex(baseInterval, interval Timestamp) int {
	// NOTE: We use interval.Sub(baseInterval) here instead of
	// interval - baseInterval since the latter produces
	// wrong values because of underflow when interval < baseInterval.
	// Another solution would be (int64(interval) - int64(baseInterval))
	pointDistance := int64(interval.Sub(baseInterval)) / int64(r.secondsPerPoint)
	return int(floorMod(pointDistance, int64(r.numberOfPoints)))
}

func (r *Retention) MaxRetention() Duration {
	return r.secondsPerPoint * Duration(r.numberOfPoints)
}

func (r *Retention) pointOffsetAt(index int) uint32 {
	return r.offset + uint32(index)*pointSize
}

func (r *Retention) Interval(t Timestamp) Timestamp {
	step := int64(r.secondsPerPoint)
	return Timestamp(int64(t) - floorMod(int64(t), step) + step)
}

func (r *Retention) intervalForWrite(t Timestamp) Timestamp {
	step := int64(r.secondsPerPoint)
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
func (r *Retention) alignPoints(points []Point) []Point {
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
