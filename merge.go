package whispertool

import (
	"errors"
	"fmt"
	"math"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func Merge(src, dest string, recursive bool, now, from, until time.Time) error {
	if recursive {
		return errors.New("recursive option not implemented yet")
	}

	readFrom := time.Unix(0, 0)
	readUntil := now

	srcData, err := readWhisperFile(src, now, readFrom, readUntil)
	if err != nil {
		return err
	}

	opts := &whisper.Options{FLock: true}
	destDB, err := whisper.OpenWithOptions(dest, opts)
	if err != nil {
		return err
	}
	defer destDB.Close()

	destData, err := readWhisperDB(destDB, now, readFrom, readUntil)
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

	tss := buildMultiTimeSeriesPointsPointersForMerge(srcData.tss, destData.tss, int(from.Unix()), int(until.Unix()), srcData.retentions)
	return updateWhisperFile(destDB, tss)
}

func buildTimeSeriesPointsPointersForMerge(srcTs, destTs []*whisper.TimeSeriesPoint, from, until int, propagatedTs []int64) []*whisper.TimeSeriesPoint {
	var ts []*whisper.TimeSeriesPoint
	for i, srcPt := range srcTs {
		var propagateCopy bool
		if len(propagatedTs) > 0 && propagatedTs[0] == int64(srcPt.Time) {
			propagateCopy = true
			propagatedTs = propagatedTs[1:]
		}
		if ((from <= srcPt.Time && math.IsNaN(destTs[i].Value)) || propagateCopy) && !math.IsNaN(srcPt.Value) {
			ts = append(ts, srcPt)
		}
		if until < srcPt.Time {
			break
		}
	}
	return ts
}

func buildPropagatedTs(highTs []*whisper.TimeSeriesPoint, step int) []int64 {
	var ts []int64
	for _, highPt := range highTs {
		t := alignUnixTime(int64(highPt.Time), step)
		if len(ts) == 0 || ts[len(ts)-1] != t {
			ts = append(ts, t)
		}
	}
	return ts
}

func buildMultiTimeSeriesPointsPointersForMerge(srcTss, destTss [][]*whisper.TimeSeriesPoint, from, until int, retentions []whisper.Retention) [][]*whisper.TimeSeriesPoint {
	var propagatedTs []int64
	tss := make([][]*whisper.TimeSeriesPoint, len(srcTss))
	for i, srcTs := range srcTss {
		if i > 0 {
			propagatedTs = buildPropagatedTs(tss[i-1], retentions[i].SecondsPerPoint())
		}
		tss[i] = buildTimeSeriesPointsPointersForMerge(srcTs, destTss[i], from, until, propagatedTs)
	}
	return tss
}
