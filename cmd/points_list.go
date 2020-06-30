package cmd

import (
	"fmt"
	"io"

	"github.com/hnakamur/whispertool"
)

type PointsList []whispertool.Points

func (pp PointsList) Print(w io.Writer) error {
	for i, points := range pp {
		for _, p := range points {
			_, err := fmt.Fprintf(w, "retID:%d\tt:%s\tval:%s\n", i, p.Time, p.Value)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (pl PointsList) AllEmpty() bool {
	for _, pts := range pl {
		if len(pts) != 0 {
			return false
		}
	}
	return true
}

func (pl PointsList) Counts() []int {
	counts := make([]int, len(pl))
	for i, pts := range pl {
		counts[i] = len(pts)
	}
	return counts
}

func (pl PointsList) Diff(ql []whispertool.Points) ([]whispertool.Points, []whispertool.Points) {
	if len(pl) != len(ql) {
		return pl, ql
	}

	pl2 := make([]whispertool.Points, len(pl))
	ql2 := make([]whispertool.Points, len(ql))
	for i, pp := range pl {
		pl2[i], ql2[i] = pp.Diff(ql[i])
	}
	return pl2, ql2
}
