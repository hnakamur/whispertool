package whispertool

import (
	"errors"
	"fmt"
	"math"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func Merge(src, dest string, recursive bool) error {
	if recursive {
		return errors.New("recursive option not implemented yet")
	}

	now := time.Now()
	srcData, err := readWhisperFile(src, now)
	if err != nil {
		return err
	}

	opts := &whisper.Options{FLock: true}
	destDB, err := whisper.OpenWithOptions(dest, opts)
	if err != nil {
		return err
	}
	defer destDB.Close()

	destData, err := readWhisperDB(destDB, now)
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

	tss := buildMultiTimeSeriesPointsPointersForMerge(srcData.tss, destData.tss)
	return updateWhisperFile(destDB, tss)
}

func buildTimeSeriesPointsPointersForMerge(srcTs, destTs []*whisper.TimeSeriesPoint) []*whisper.TimeSeriesPoint {
	var ts []*whisper.TimeSeriesPoint
	for i, srcPt := range srcTs {
		if math.IsNaN(destTs[i].Value) && !math.IsNaN(srcPt.Value) {
			ts = append(ts, srcPt)
		}
	}
	return ts
}

func buildMultiTimeSeriesPointsPointersForMerge(srcTss, destTss [][]*whisper.TimeSeriesPoint) [][]*whisper.TimeSeriesPoint {
	tss := make([][]*whisper.TimeSeriesPoint, len(srcTss))
	for i, srcTs := range srcTss {
		tss[i] = buildTimeSeriesPointsPointersForMerge(srcTs, destTss[i])
	}
	return tss
}
