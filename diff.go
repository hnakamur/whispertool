package whispertool

import (
	"errors"
	"fmt"
	"math"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

var ErrDiffFound = errors.New("diff found")

func Diff(src, dest string, recursive, ignoreSrcEmpty, showAll bool, now, from, until time.Time) error {
	if recursive {
		return errors.New("recursive option not implemented yet")
	}

	srcData, err := readWhisperFile(src, now, from, until)
	if err != nil {
		return err
	}

	destData, err := readWhisperFile(dest, now, from, until)
	if err != nil {
		return err
	}

	if !retentionsEqual(srcData.retentions, destData.retentions) {
		return fmt.Errorf("%s and %s archive confiugrations are unalike. "+
			"Resize the input before diffing", src, dest)
	}

	if !timeEqualMultiTimeSeriesPointsPointers(srcData.tss, destData.tss) {
		return fmt.Errorf("%s and %s archive time values are unalike. "+
			"Resize the input before diffing", src, dest)
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
				fmt.Printf("retId:%d\tt:%s\tsrcVal:%g\tdestVal:%g\tdiff:%d\n",
					i,
					formatTime(secondsToTime(int64(srcPt.Time))),
					srcPt.Value,
					destPt.Value,
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
			fmt.Printf("retId:%d\tt:%s\tsrcVal:%g\tdestVal:%g\n",
				i,
				formatTime(secondsToTime(int64(srcPt.Time))),
				srcPt.Value,
				destPt.Value)
		}
	}

	return ErrDiffFound
}

func retentionsEqual(rr1, rr2 []whisper.Retention) bool {
	if len(rr1) != len(rr2) {
		return false
	}
	for i, r1 := range rr1 {
		if r1.String() != rr2[i].String() {
			return false
		}
	}
	return true
}

func valueEqualTimeSeriesPoint(src, dest *whisper.TimeSeriesPoint, ignoreSrcEmpty bool) bool {
	srcVal := src.Value
	srcIsNaN := math.IsNaN(srcVal)
	if srcIsNaN && ignoreSrcEmpty {
		return true
	}

	destVal := dest.Value
	destIsNaN := math.IsNaN(destVal)
	return srcIsNaN && destIsNaN ||
		(!srcIsNaN && !destIsNaN && srcVal == destVal)
}

func valueDiffIndexesTimeSeriesPointsPointers(src, dest []*whisper.TimeSeriesPoint, ignoreSrcEmpty bool) []int {
	var is []int
	for i, srcPt := range src {
		destPt := dest[i]
		if !valueEqualTimeSeriesPoint(srcPt, destPt, ignoreSrcEmpty) {
			is = append(is, i)
		}
	}
	return is
}

func valueDiffIndexesMultiTimeSeriesPointsPointers(src, dest [][]*whisper.TimeSeriesPoint, ignoreSrcEmpty bool) [][]int {
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

func timeEqualTimeSeriesPointsPointers(src, dest []*whisper.TimeSeriesPoint) bool {
	if len(src) != len(dest) {
		return false
	}

	for i, srcPt := range src {
		destPt := dest[i]
		if srcPt == nil || destPt == nil || srcPt.Time != destPt.Time {
			return false
		}
	}

	return true
}

func timeEqualMultiTimeSeriesPointsPointers(src, dest [][]*whisper.TimeSeriesPoint) bool {
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
