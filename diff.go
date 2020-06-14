package whispertool

import (
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"time"
)

var ErrDiffFound = errors.New("diff found")

func Diff(src, dest string, recursive, ignoreSrcEmpty, showAll bool, now, from, until time.Time) error {
retry:
	if recursive {
		return errors.New("recursive option not implemented yet")
	}

	srcData, err := readWhisperFile(src, now, from, until, RetIdAll)
	if err != nil {
		return err
	}

	destData, err := readWhisperFile(dest, now, from, until, RetIdAll)
	if err != nil {
		return err
	}

	if !retentionsEqual(srcData.retentions, destData.retentions) {
		return fmt.Errorf("%s and %s archive confiugrations are unalike. "+
			"Resize the input before diffing", src, dest)
	}

	if err := timeDiffMultiTimeSeriesPointsPointers(srcData.tss, destData.tss); err != nil {
		log.Printf("diff failed since %s and %s archive time values are unalike: %s",
			src, dest, err.Error())
		goto retry
	}

	iss := valueDiffIndexesMultiTimeSeriesPointsPointers(srcData.tss, destData.tss, ignoreSrcEmpty)
	if diffIndexesEmpty(iss) {
		return nil
	}

	if showAll {
		for i, is := range iss {
			srcTs := srcData.tss[i]
			destTs := destData.tss[i]
			for j, srcPt := range srcTs {
				var diff int
				if len(is) > 0 && is[0] == j {
					diff = 1
					is = is[1:]
				}
				destPt := destTs[j]
				fmt.Printf("retId:%d\tt:%s\tsrcVal:%s\tdestVal:%s\tdiff:%d\n",
					i,
					formatTime(secondsToTime(int64(srcPt.Time))),
					strconv.FormatFloat(srcPt.Value, 'f', -1, 64),
					strconv.FormatFloat(destPt.Value, 'f', -1, 64),
					diff)
			}
		}
		return ErrDiffFound
	}

	for i, is := range iss {
		srcTs := srcData.tss[i]
		destTs := destData.tss[i]
		for _, j := range is {
			srcPt := srcTs[j]
			destPt := destTs[j]
			fmt.Printf("retId:%d\tt:%s\tsrcVal:%s\tdestVal:%s\n",
				i,
				formatTime(secondsToTime(int64(srcPt.Time))),
				strconv.FormatFloat(srcPt.Value, 'f', -1, 64),
				strconv.FormatFloat(destPt.Value, 'f', -1, 64))
		}
	}

	return ErrDiffFound
}

func retentionsEqual(rr1, rr2 []Retention) bool {
	if len(rr1) != len(rr2) {
		return false
	}
	for i, r1 := range rr1 {
		r2 := rr2[i]
		if r1.SecondsPerPoint != r2.SecondsPerPoint || r1.NumberOfPoints != r2.NumberOfPoints {
			return false
		}
	}
	return true
}

func valueEqualTimeSeriesPoint(src, dest Point, ignoreSrcEmpty bool) bool {
	srcVal := src.Value
	srcIsNaN := math.IsNaN(srcVal)
	if srcIsNaN && ignoreSrcEmpty {
		return true
	}

	destVal := dest.Value
	destIsNaN := math.IsNaN(destVal)
	return (srcIsNaN && destIsNaN) ||
		(!srcIsNaN && !destIsNaN && srcVal == destVal)
}

func valueDiffIndexesTimeSeriesPointsPointers(src, dest []Point, ignoreSrcEmpty bool) []int {
	var is []int
	for i, srcPt := range src {
		destPt := dest[i]
		if !valueEqualTimeSeriesPoint(srcPt, destPt, ignoreSrcEmpty) {
			is = append(is, i)
		}
	}
	return is
}

func valueDiffIndexesMultiTimeSeriesPointsPointers(src, dest [][]Point, ignoreSrcEmpty bool) [][]int {
	iss := make([][]int, len(src))
	for i, srcTs := range src {
		destTs := dest[i]
		iss[i] = valueDiffIndexesTimeSeriesPointsPointers(srcTs, destTs, ignoreSrcEmpty)
	}
	return iss
}

func diffIndexesEmpty(iss [][]int) bool {
	for _, is := range iss {
		if len(is) != 0 {
			return false
		}
	}
	return true
}

func timeEqualTimeSeriesPointsPointers(src, dest []Point) bool {
	if len(src) != len(dest) {
		return false
	}

	for i, srcPt := range src {
		destPt := dest[i]
		if srcPt.Time != destPt.Time {
			return false
		}
	}

	return true
}

func timeEqualMultiTimeSeriesPointsPointers(src, dest [][]Point) bool {
	if len(src) != len(dest) {
		return false
	}

	for i, srcTs := range src {
		if !timeEqualTimeSeriesPointsPointers(srcTs, dest[i]) {
			return false
		}
	}

	return true
}

func timeDiffTimeSeriesPointsPointers(src, dest []Point) error {
	if len(src) != len(dest) {
		return fmt.Errorf("point count unmatch, src=%d, dest=%d", len(src), len(dest))
	}

	for i, srcPt := range src {
		destPt := dest[i]
		if srcPt.Time != destPt.Time {
			return fmt.Errorf("point %d time unmatch src=%s, dest=%s",
				i,
				formatTime(secondsToTime(int64(srcPt.Time))),
				formatTime(secondsToTime(int64(destPt.Time))))
		}
	}

	return nil
}

func timeDiffMultiTimeSeriesPointsPointers(src, dest [][]Point) error {
	if len(src) != len(dest) {
		return fmt.Errorf("retention count unmatch, src=%d, dest=%d", len(src), len(dest))
	}

	for i, srcTs := range src {
		if err := timeDiffTimeSeriesPointsPointers(srcTs, dest[i]); err != nil {
			return fmt.Errorf("timestamps unmatch for retention=%d: %s", i, err)
		}
	}

	return nil
}
