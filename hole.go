package whispertool

import (
	"time"
)

func Hole(src, dest string, emptyRate float64, now, from, until time.Time) error {
	//	tsNow := TimestampFromStdTime(now)
	//	tsFrom := TimestampFromStdTime(from)
	//	tsUntil := TimestampFromStdTime(until)
	//	log.Printf("Hole, tsNow=%s, tsFrom=%s, tsUntil=%s", tsNow, tsFrom, tsUntil)
	//
	//	d, err := readWhisperFile(src, tsNow, tsFrom, tsUntil, RetIdAll)
	//	if err != nil {
	//		return err
	//	}
	//
	//	rnd := rand.New(rand.NewSource(newRandSeed()))
	//	emptyRandomPointsInTimeSeriesPointsForAllArchives(d.pointsList, rnd, emptyRate, tsFrom, tsUntil, d.retentions)
	//
	//	if err = writeWhisperFileData("dest.txt", d, true); err != nil {
	//		return err
	//	}
	//
	//	return createWhisperFile(dest, d, tsNow)
	return nil
}

//func emptyRandomPointsInTimeSeriesPoints(ts []Point, rnd *rand.Rand, empyRate float64, from, until Timestamp, retention *Retention) []Point {
//	var ts2 []Point
//	step := retention.SecondsPerPoint
//	fromInterval := alignTime(from, step)
//	untilInterval := alignTime(until, step)
//	for _, p := range ts {
//		if fromInterval <= p.Time && p.Time <= untilInterval && rnd.Float64() < empyRate {
//			continue
//		}
//		ts2 = append(ts2, Point{
//			Time:  p.Time,
//			Value: p.Value,
//		})
//	}
//	return ts2
//}
//
//func emptyRandomPointsInTimeSeriesPointsForAllArchives(tss [][]Point, rnd *rand.Rand, emptyRate float64, from, until Timestamp, retentions []Retention) {
//	for i, ts := range tss {
//		r := &retentions[i]
//		tss[i] = emptyRandomPointsInTimeSeriesPoints(ts, rnd, emptyRate, from, until, r)
//	}
//}
