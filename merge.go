package whispertool

import (
	"errors"
	"fmt"
	"os"
	"time"
)

func Merge(src, dest string, recursive bool, now, from, until time.Time) error {
	if recursive {
		return errors.New("recursive option not implemented yet")
	}

	tsNow := TimestampFromStdTime(now)
	tsFrom := TimestampFromStdTime(from)
	tsUntil := TimestampFromStdTime(until)

	srcData, err := readWhisperFile(src, tsNow, tsFrom, tsUntil, RetIdAll)
	if err != nil {
		return err
	}

	p := NewBufferPool(os.Getpagesize())
	destDB, err := OpenForWrite(dest, p)
	if err != nil {
		return err
	}
	defer destDB.Close()

	destData, err := readWhisperDB(destDB, tsNow, tsFrom, tsUntil, RetIdAll, dest)
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

	tss := buildMultiTimeSeriesPointsPointersForMerge(srcData.tss, destData.tss, tsFrom, tsUntil, srcData.retentions)
	return updateWhisperFile(destDB, tss)
	return nil
}

func buildTimeSeriesPointsPointersForMerge(srcTs, destTs []Point, from, until Timestamp, propagatedTs []Timestamp) []Point {
	var ts []Point
	for i, srcPt := range srcTs {
		var propagateCopy bool
		if len(propagatedTs) > 0 && propagatedTs[0] == srcPt.Time {
			propagateCopy = true
			propagatedTs = propagatedTs[1:]
		}
		if (from <= srcPt.Time && destTs[i].Value.IsNaN() || propagateCopy) && !srcPt.Value.IsNaN() {
			ts = append(ts, srcPt)
		}
		if until < srcPt.Time {
			break
		}
	}
	return ts
}

func buildPropagatedTs(highTs []Point, step Duration) []Timestamp {
	var ts []Timestamp
	for _, highPt := range highTs {
		t := alignTime(highPt.Time, step)
		if len(ts) == 0 || ts[len(ts)-1] != t {
			ts = append(ts, t)
		}
	}
	return ts
}

func buildMultiTimeSeriesPointsPointersForMerge(srcTss, destTss [][]Point, from, until Timestamp, retentions []Retention) [][]Point {
	var propagatedTs []Timestamp
	tss := make([][]Point, len(srcTss))
	for i, srcTs := range srcTss {
		if i > 0 {
			propagatedTs = buildPropagatedTs(tss[i-1], retentions[i].SecondsPerPoint)
		}
		tss[i] = buildTimeSeriesPointsPointersForMerge(srcTs, destTss[i], from, until, propagatedTs)
	}
	return tss
}
