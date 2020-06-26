package whispertool

import (
	"errors"
	"fmt"
	"log"
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
	log.Printf("merge, tsNow=%s, tsFrom=%s, tsUntil=%s", tsNow, tsFrom, tsUntil)

	srcData, err := readWhisperFile(src, tsNow, tsFrom, tsUntil, RetIdAll)
	if err != nil {
		return err
	}

	textOut := "src-before-merge.txt"
	if err = writeWhisperFileData(textOut, srcData, true); err != nil {
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

	textOut = "dest-before-merge.txt"
	if err = writeWhisperFileData(textOut, destData, true); err != nil {
		return err
	}

	if !retentionsEqual(srcData.retentions, destData.retentions) {
		return fmt.Errorf("%s and %s archive confiugrations are unalike. "+
			"Resize the input before merging", src, dest)
	}

	if err := timeDiffMultiArchivePoints(srcData.tss, destData.tss); err != nil {
		log.Printf("merge failed since %s and %s archive time values are unalike: %s",
			src, dest, err.Error())
		return fmt.Errorf("merge failed since %s and %s archive time values are unalike: %s",
			src, dest, err.Error())
	}
	//if !timeEqualMultiPoints(srcData.tss, destData.tss) {
	//	return fmt.Errorf("%s and %s archive time values are unalike. "+
	//		"Resize the input before merging", src, dest)
	//}

	tss := buildMultiTimeSeriesPointsPointersForMerge(srcData.tss, destData.tss, tsFrom, tsUntil, srcData.retentions)

	log.Printf("len(tss)=%d", len(tss))
	for i, ts := range tss {
		log.Printf("i=%d, len(ts)=%d", i, len(ts))
		for j, p := range ts {
			log.Printf("i=%d, j=%d, p.Time=%s, p.Value=%s", i, j, p.Time, p.Value)
		}
	}

	if err := updateWhisperFile(destDB, tss, tsNow); err != nil {
		return err
	}
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
