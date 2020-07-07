package cmd

import (
	"fmt"
	"io"

	"github.com/hnakamur/whispertool"
)

type TimeSeriesList []*whispertool.TimeSeries

func (tl TimeSeriesList) Print(w io.Writer) error {
	for retID, ts := range tl {
		for i, v := range ts.Values() {
			t := ts.FromTime().Add(whispertool.Duration(i) * ts.Step())
			_, err := fmt.Fprintf(w, "retID:%d\tt:%s\tval:%s\n", retID, t, v)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (tl TimeSeriesList) AllEmpty() bool {
	for _, ts := range tl {
		if len(ts.Values()) != 0 {
			return false
		}
	}
	return true
}

func (tl TimeSeriesList) PointCounts() []int {
	counts := make([]int, len(tl))
	for i, ts := range tl {
		counts[i] = len(ts.Values())
	}
	return counts
}

// AllEqualTimeRangeAndStep returns whether or not all of
// FromTime(), UntilTime() and Step() are the same
// between all TimeSeries in tl and ul.
func (tl TimeSeriesList) AllEqualTimeRangeAndStep(ul TimeSeriesList) bool {
	if len(tl) != len(ul) {
		return false
	}
	for i, ts := range tl {
		if !ts.EqualTimeRangeAndStep(ul[i]) {
			return false
		}
	}
	return true
}

func (tl TimeSeriesList) PointsList() PointsList {
	pl := make(PointsList, len(tl))
	for i, ts := range tl {
		pl[i] = ts.Points()
	}
	return pl
}

func (tl TimeSeriesList) Diff(ul TimeSeriesList) (PointsList, PointsList) {
	if len(tl) != len(ul) {
		return tl.PointsList(), ul.PointsList()
	}

	pl2 := make(PointsList, len(tl))
	ql2 := make(PointsList, len(ul))
	for i, ts := range tl {
		pl2[i], ql2[i] = ts.DiffPoints(ul[i])
	}
	return pl2, ql2
}
