package whispertool

import (
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

const UTCTimeLayout = "2006-01-02T15:04:05Z"

func formatUnixTime(t int) string {
	return time.Unix(int64(t), 0).UTC().Format(UTCTimeLayout)
}

func formatTsPoint(p whisper.TimeSeriesPoint) string {
	return fmt.Sprintf("{Time:%s Value:%g}",
		formatUnixTime(p.Time), p.Value)
}

func formatTsPoints(pts []whisper.TimeSeriesPoint) string {
	var b strings.Builder
	for i, p := range pts {
		if i > 0 {
			b.WriteRune(' ')
		}
		b.WriteString(formatTsPoint(p))
	}
	return b.String()
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

func tsPointEqual(srcPt, destPt whisper.TimeSeriesPoint, ignoreSrcEmpty bool) bool {
	if srcPt.Time != destPt.Time {
		return false
	}

	srcVal := srcPt.Value
	srcIsNaN := math.IsNaN(srcVal)
	if srcIsNaN && ignoreSrcEmpty {
		return true
	}

	destVal := destPt.Value
	destIsNaN := math.IsNaN(destVal)
	return srcIsNaN && destIsNaN ||
		(!srcIsNaN && !destIsNaN && srcVal == destVal)
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

func Run(src, dest string, untilTime, now int, ignoreSrcEmpty bool) error {
	oflag := os.O_RDONLY
	opts := &whisper.Options{OpenFileFlag: &oflag}

	srcDB, err := whisper.OpenWithOptions(src, opts)
	if err != nil {
		return err
	}
	defer srcDB.Close()

	destDB, err := whisper.OpenWithOptions(dest, opts)
	if err != nil {
		return err
	}
	defer destDB.Close()

	if !retentionsEqual(srcDB.Retentions(), destDB.Retentions()) {
		return fmt.Errorf("%s and %s archive confiugrations are unalike. "+
			"Resize the input before diffing", src, dest)
	}

	for i, srcRet := range srcDB.Retentions() {
		fromTime := now - srcRet.MaxRetention()
		if fromTime >= untilTime-srcRet.SecondsPerPoint() {
			continue
		}
		fmt.Printf("i=%d, srcRet=%s, r2=%s\n", i, srcRet, destDB.Retentions()[i])
		srcTs, err := srcDB.Fetch(fromTime, untilTime)
		if err != nil {
			return err
		}
		destTs, err := destDB.Fetch(fromTime, untilTime)
		if err != nil {
			return err
		}

		if srcTs.Step() != destTs.Step() {
			return fmt.Errorf("diffing timeseries with unmatched steps is not supported, fromTime=%s, untilTime=%s, srcStep=%d, destStep=%d, srcFrom=%s, destFrom=%s, srcUntil=%s, destUntil=%s",
				formatUnixTime(fromTime),
				formatUnixTime(untilTime),
				srcTs.Step(),
				destTs.Step(),
				formatUnixTime(srcTs.FromTime()),
				formatUnixTime(destTs.FromTime()),
				formatUnixTime(srcTs.UntilTime()),
				formatUnixTime(destTs.UntilTime()))
		}
		if srcTs.FromTime() != destTs.FromTime() {
			return fmt.Errorf("diffing timeseries with unmatched fromTime is not supported, fromTime=%s, untilTime=%s, srcStep=%d, destStep=%d, srcFrom=%s, destFrom=%s, srcUntil=%s, destUntil=%s",
				formatUnixTime(fromTime),
				formatUnixTime(untilTime),
				srcTs.Step(),
				destTs.Step(),
				formatUnixTime(srcTs.FromTime()),
				formatUnixTime(destTs.FromTime()),
				formatUnixTime(srcTs.UntilTime()),
				formatUnixTime(destTs.UntilTime()))
		}
		if srcTs.UntilTime() != destTs.UntilTime() {
			return fmt.Errorf("diffing timeseries with unmatched untilTime is not supported, fromTime=%s, untilTime=%s, srcStep=%d, destStep=%d, srcFrom=%s, destFrom=%s, srcUntil=%s, destUntil=%s",
				formatUnixTime(fromTime),
				formatUnixTime(untilTime),
				srcTs.Step(),
				destTs.Step(),
				formatUnixTime(srcTs.FromTime()),
				formatUnixTime(destTs.FromTime()),
				formatUnixTime(srcTs.UntilTime()),
				formatUnixTime(destTs.UntilTime()))
		}
		fmt.Printf("fromTime: arg=%s, srcTs=%s, destTs=%s\n",
			formatUnixTime(fromTime),
			formatUnixTime(srcTs.FromTime()),
			formatUnixTime(destTs.FromTime()))
		fmt.Printf("untilTime: arg=%s, srcTs=%s, destTs=%s\n",
			formatUnixTime(untilTime),
			formatUnixTime(srcTs.UntilTime()),
			formatUnixTime(destTs.UntilTime()))
		fmt.Printf("step: srcTs=%d, destTs=%d\n",
			srcTs.Step(), destTs.Step())
		fmt.Printf("srcTs.Points=%s\n", formatTsPoints(srcTs.Points()))
		fmt.Printf("destTs.Points=%s\n", formatTsPoints(destTs.Points()))

		if untilTime > fromTime {
			untilTime = fromTime
		}
	}

	return nil
}
