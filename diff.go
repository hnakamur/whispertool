package whispertool

import (
	"errors"
	"fmt"
	"log"
	"time"
)

var ErrDiffFound = errors.New("diff found")

func Diff(src, dest string, recursive, ignoreSrcEmpty, ignoreDestEmpty, showAll bool, now, from, until time.Time, retId int) error {
	if recursive {
		return errors.New("recursive option not implemented yet")
	}

	tsNow := TimestampFromStdTime(now)
	tsFrom := TimestampFromStdTime(from)
	tsUntil := TimestampFromStdTime(until)
	srcData, err := readWhisperFile(src, tsNow, tsFrom, tsUntil, retId)
	if err != nil {
		return err
	}

	destData, err := readWhisperFile(dest, tsNow, tsFrom, tsUntil, retId)
	if err != nil {
		return err
	}

	iss, err := diffIndexesWhisperFileData(srcData, destData, ignoreSrcEmpty, ignoreDestEmpty, showAll, retId)
	if err != nil {
		return err
	}

	if diffIndexesEmpty(iss) {
		return nil
	}
	writeDiff(iss, srcData, destData, showAll)
	return ErrDiffFound
}

func diffIndexesWhisperFileData(src, dest *whisperFileData, ignoreSrcEmpty, ignoreDestEmpty, showAll bool, retId int) ([][]int, error) {
	if !retentionsEqual(src.retentions, dest.retentions) {
		return nil, fmt.Errorf("%s and %s archive confiugrations are unalike. "+
			"Resize the input before diffing", src.filename, dest.filename)
	}

	if err := timeDiffMultiArchivePoints(src.tss, dest.tss); err != nil {
		log.Printf("diff failed since %s and %s archive time values are unalike: %s",
			src.filename, dest.filename, err.Error())
		//goto retry
		return nil, fmt.Errorf("diff failed since %s and %s archive time values are unalike: %s",
			src.filename, dest.filename, err.Error())
	}

	iss := valueDiffIndexesMultiArchivePoints(src.tss, dest.tss, ignoreSrcEmpty, ignoreDestEmpty)
	return iss, nil
}

func writeDiff(indexes [][]int, src, dest *whisperFileData, showAll bool) {
	if showAll {
		for i, is := range indexes {
			srcTs := src.tss[i]
			destTs := dest.tss[i]
			for j, srcPt := range srcTs {
				var diff int
				if len(is) > 0 && is[0] == j {
					diff = 1
					is = is[1:]
				}
				destPt := destTs[j]
				fmt.Printf("retId:%d\tt:%s\tsrcVal:%s\tdestVal:%s\tdiff:%d\n",
					i, srcPt.Time, srcPt.Value, destPt.Value, diff)
			}
		}
		return
	}

	for i, is := range indexes {
		srcTs := src.tss[i]
		destTs := dest.tss[i]
		for _, j := range is {
			srcPt := srcTs[j]
			destPt := destTs[j]
			fmt.Printf("retId:%d\tt:%s\tsrcVal:%s\tdestVal:%s\n",
				i, srcPt.Time, srcPt.Value, destPt.Value)
		}
	}
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

func valueEqualTimeSeriesPoint(src, dest Point, ignoreSrcEmpty, ignoreDestEmpty bool) bool {
	srcVal := src.Value
	srcIsNaN := srcVal.IsNaN()
	if srcIsNaN && ignoreSrcEmpty {
		return true
	}

	destVal := dest.Value
	destIsNaN := destVal.IsNaN()
	return ((srcIsNaN || ignoreDestEmpty) && destIsNaN) ||
		(!srcIsNaN && !destIsNaN && srcVal == destVal)
}

func valueDiffIndexesPoints(src, dest []Point, ignoreSrcEmpty, ignoreDestEmpty bool) []int {
	var is []int
	for i, srcPt := range src {
		destPt := dest[i]
		if !valueEqualTimeSeriesPoint(srcPt, destPt, ignoreSrcEmpty, ignoreDestEmpty) {
			is = append(is, i)
		}
	}
	return is
}

func valueDiffIndexesMultiArchivePoints(src, dest [][]Point, ignoreSrcEmpty, ignoreDestEmpty bool) [][]int {
	iss := make([][]int, len(src))
	for i, srcTs := range src {
		destTs := dest[i]
		iss[i] = valueDiffIndexesPoints(srcTs, destTs, ignoreSrcEmpty, ignoreDestEmpty)
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

func timeEqualPoints(src, dest []Point) bool {
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

func timeEqualMultiPoints(src, dest [][]Point) bool {
	if len(src) != len(dest) {
		return false
	}

	for i, srcTs := range src {
		if !timeEqualPoints(srcTs, dest[i]) {
			return false
		}
	}

	return true
}

func timeDiffPoints(src, dest []Point) error {
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

func timeDiffMultiArchivePoints(src, dest [][]Point) error {
	if len(src) != len(dest) {
		return fmt.Errorf("retention count unmatch, src=%d, dest=%d", len(src), len(dest))
	}

	for i, srcTs := range src {
		if err := timeDiffPoints(srcTs, dest[i]); err != nil {
			return fmt.Errorf("timestamps unmatch for retention=%d: %s", i, err)
		}
	}

	return nil
}
