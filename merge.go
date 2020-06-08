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

	srcData, err := readWhisperFile(src, now, from, until)
	if err != nil {
		return err
	}

	opts := &whisper.Options{FLock: true}
	destDB, err := whisper.OpenWithOptions(dest, opts)
	if err != nil {
		return err
	}
	defer destDB.Close()

	destData, err := readWhisperDB(destDB, now, from, until)
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

	tss := buildMultiTimeSeriesPointsPointersForMerge(srcData.tss, destData.tss, srcData.retentions)
	return updateWhisperFile(destDB, tss)
}

func buildTimeSeriesPointsPointersForMerge(srcTs, destTs []*whisper.TimeSeriesPoint, propagatedTs []int64) []*whisper.TimeSeriesPoint {
	var ts []*whisper.TimeSeriesPoint
	for i, srcPt := range srcTs {
		var propagateCopy bool
		if len(propagatedTs) > 0 && propagatedTs[0] == int64(srcPt.Time) {
			propagateCopy = true
			propagatedTs = propagatedTs[1:]
		}
		if (math.IsNaN(destTs[i].Value) || propagateCopy) && !math.IsNaN(srcPt.Value) {
			ts = append(ts, srcPt)
		}
	}
	return ts
}

func buildPropagatedTs(highTs []*whisper.TimeSeriesPoint, step int) []int64 {
	var ts []int64
	for _, highPt := range highTs {
		t := alignTime(secondsToTime(int64(highPt.Time)), step).Unix()
		if len(ts) == 0 || ts[len(ts)-1] != t {
			ts = append(ts, t)
		}
	}
	return ts
}

func buildMultiTimeSeriesPointsPointersForMerge(srcTss, destTss [][]*whisper.TimeSeriesPoint, retentions []whisper.Retention) [][]*whisper.TimeSeriesPoint {
	var propagatedTs []int64
	tss := make([][]*whisper.TimeSeriesPoint, len(srcTss))
	for i, srcTs := range srcTss {
		if i > 0 {
			propagatedTs = buildPropagatedTs(tss[i-1], retentions[i].SecondsPerPoint())
		}
		tss[i] = buildTimeSeriesPointsPointersForMerge(srcTs, destTss[i], propagatedTs)
	}
	return tss
}
