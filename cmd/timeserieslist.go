package cmd

import (
	"fmt"
	"io"
	"strings"

	"github.com/hnakamur/whispertool"
)

type TimeSeriesList []*whispertool.TimeSeries

func (tl TimeSeriesList) Print(w io.Writer) error {
	for archiveID, ts := range tl {
		for i, v := range ts.Values() {
			t := ts.FromTime().Add(whispertool.Duration(i) * ts.Step())
			_, err := fmt.Fprintf(w, "archiveID:%d\tt:%s\tval:%s\n", archiveID, t, v)
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

// Equal returns whether or not tl equals to tl2.
func (tl TimeSeriesList) Equal(tl2 TimeSeriesList) bool {
	if len(tl) != len(tl2) {
		return false
	}
	for i, ts := range tl {
		ts2 := tl2[i]
		if !ts.Equal(ts2) {
			return false
		}
	}
	return true
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

func (tl TimeSeriesList) DiffExcludeSrcNaN(ul TimeSeriesList) (PointsList, PointsList) {
	if len(tl) != len(ul) {
		return tl.PointsList(), ul.PointsList()
	}

	pl2 := make(PointsList, len(tl))
	ql2 := make(PointsList, len(ul))
	for i, ts := range tl {
		pl2[i], ql2[i] = ts.DiffPointsExcludeSrcNaN(ul[i])
	}
	return pl2, ql2
}

func (tl TimeSeriesList) TimeRangeStepString() string {
	var b strings.Builder
	b.WriteString("{")
	for archiveID, ts := range tl {
		if archiveID > 0 {
			b.WriteString(" ")
		}
		fmt.Fprintf(&b, "{archiveID:%d fromTime:%s untilTime:%s step:%s}",
			archiveID, ts.FromTime(), ts.UntilTime(), ts.Step())
	}
	b.WriteString("}")
	return b.String()
}
