package whispertool

import (
	"errors"
	"math/rand"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func Hole(src, dest string, emptyRate float64, now, from, until time.Time) error {
	readFrom := time.Unix(0, 0)
	readUntil := now
	d, err := readWhisperFile(src, now, readFrom, readUntil, RetIdAll)
	if err != nil {
		return err
	}

	rnd := rand.New(rand.NewSource(newRandSeed()))
	emptyRandomPointsInTimeSeriesPointsForAllArchives(d.tss, rnd, emptyRate, from, until, d.retentions)
	return createWhisperFile(dest, d)
}

var errInvalidAggregationMethod = errors.New("invalid aggregation method")

func stringToAggregationMethod(method string) (whisper.AggregationMethod, error) {
	switch method {
	case "Average":
		return whisper.Average, nil
	case "Sum":
		return whisper.Sum, nil
	case "First":
		return whisper.First, nil
	case "Last":
		return whisper.Last, nil
	case "Max":
		return whisper.Max, nil
	case "Min":
		return whisper.Min, nil
	default:
		return 0, errInvalidAggregationMethod
	}
}

func emptyRandomPointsInTimeSeriesPoints(ts []*whisper.TimeSeriesPoint, rnd *rand.Rand, empyRate float64, from, until time.Time, retention whisper.Retention) []*whisper.TimeSeriesPoint {
	var ts2 []*whisper.TimeSeriesPoint
	step := retention.SecondsPerPoint()
	fromSec := int(alignUnixTime(from.Unix(), step))
	untilSec := int(alignUnixTime(until.Unix(), step))
	for _, p := range ts {
		if fromSec <= p.Time && p.Time <= untilSec && rnd.Float64() < empyRate {
			continue
		}
		ts2 = append(ts2, &whisper.TimeSeriesPoint{
			Time:  p.Time,
			Value: p.Value,
		})
	}
	return ts2
}

func emptyRandomPointsInTimeSeriesPointsForAllArchives(tss [][]*whisper.TimeSeriesPoint, rnd *rand.Rand, emptyRate float64, from, until time.Time, retentions []whisper.Retention) {
	for i, ts := range tss {
		tss[i] = emptyRandomPointsInTimeSeriesPoints(ts, rnd, emptyRate, from, until, retentions[i])
	}
}
