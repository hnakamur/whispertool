package whispertool

import (
	"time"
)

func Generate(dest string, retentionDefs string, fill bool, randMax int, now time.Time, textOut string) error {
	//	retentions, err := ParseRetentions(retentionDefs)
	//	if err != nil {
	//		return err
	//	}
	//
	//	d := &whisperFileData{
	//		retentions:        retentions,
	//		aggregationMethod: Sum,
	//		xFilesFactor:      0,
	//	}
	//
	//	tsNow := TimestampFromStdTime(now)
	//	if fill {
	//		rnd := rand.New(rand.NewSource(newRandSeed()))
	//		tsUntil := tsNow
	//		d.pointsList = randomPointsForArchives(retentions, tsUntil, tsNow,
	//			rnd, randMax)
	//	}
	//
	//	//textOut = "src.txt"
	//	if err = writeWhisperFileData(textOut, d, true); err != nil {
	//		return err
	//	}
	//
	//	return createWhisperFile(dest, d, tsNow)
	//}
	//
	//func newRandSeed() int64 {
	//	var b [8]byte
	//	if _, err := crand.Read(b[:]); err != nil {
	//		return time.Now().UnixNano()
	//	}
	//	return int64(binary.BigEndian.Uint64(b[:]))
	return nil
}

//func alignUnixTime(t int64, secondsPerPoint int) int64 {
//	return t - t%int64(secondsPerPoint)
//}
//
//func alignTime(t Timestamp, secondsPerPoint Duration) Timestamp {
//	return t - t%Timestamp(secondsPerPoint)
//}
//
//func randomValWithHighSum(t Timestamp, rnd *rand.Rand, highRndMax int, r, highRet *Retention, highPts []Point) Value {
//	step := r.SecondsPerPoint
//
//	v := Value(0)
//	for _, hp := range highPts {
//		thisHighTime := alignTime(hp.Time, step)
//		if thisHighTime < t {
//			continue
//		}
//		if thisHighTime > t {
//			break
//		}
//		v += hp.Value
//	}
//
//	if len(highPts) == 0 {
//		return v
//	}
//	highStartTime := highPts[0].Time
//	if t >= highStartTime {
//		return v
//	}
//	n := int(highStartTime.Sub(t) / Second / highRet.SecondsPerPoint)
//	v2 := Value(n * rnd.Intn(highRndMax+1))
//	return v + v2
//}
//
//func randomTimeSeriesPoints(until, now Timestamp, r, highRet *Retention, rnd *rand.Rand, rndMax, highRndMax int, highPts []Point) []Point {
//	// adjust now and until for this archive
//	step := r.SecondsPerPoint
//	thisNow := alignTime(now, step)
//	thisUntil := alignTime(until, step)
//
//	var thisHighStartTime Timestamp
//	if highPts != nil {
//		highStartTime := highPts[0].Time
//		if highStartTime < thisUntil {
//			thisHighStartTime = alignTime(highStartTime, step)
//		}
//	}
//
//	n := int((r.MaxRetention() - thisNow.Sub(thisUntil)) / r.SecondsPerPoint)
//	points := make([]Point, n)
//	for i := 0; i < n; i++ {
//		t := thisUntil.Add(-Duration(n-1-i) * step * Second)
//		var v Value
//		if thisHighStartTime == 0 || t < thisHighStartTime {
//			v = Value(rnd.Intn(rndMax + 1))
//		} else {
//			v = randomValWithHighSum(t, rnd, highRndMax, r, highRet, highPts)
//		}
//		points[i] = Point{
//			Time:  t,
//			Value: v,
//		}
//	}
//	return points
//}
//
//func randomPointsForArchives(retentions []Retention, until, now Timestamp, rnd *rand.Rand, rndMaxForHightestArchive int) [][]Point {
//	tss := make([][]Point, len(retentions))
//	var highRet *Retention
//	var highRndMax int
//	var highPts []Point
//	for i := range retentions {
//		r := &retentions[i]
//		rndMax := rndMaxForHightestArchive * int(r.SecondsPerPoint) / int(retentions[0].SecondsPerPoint)
//		tss[i] = randomTimeSeriesPoints(until, now, r, highRet, rnd, rndMax, highRndMax, highPts)
//
//		highRndMax = rndMax
//		highPts = tss[i]
//		highRet = r
//	}
//	return tss
//}
//
//func createWhisperFile(filename string, d *whisperFileData, now Timestamp) error {
//	p := NewBufferPool(os.Getpagesize())
//	db := &Whisper{
//		Meta: Meta{
//			AggregationMethod: d.aggregationMethod,
//			XFilesFactor:      d.xFilesFactor,
//		},
//		Retentions: d.retentions,
//	}
//	err := db.Create(filename, p, 0644)
//	if err != nil {
//		return err
//	}
//	defer db.Close()
//
//	if err := updateWhisperFile(db, d.pointsList, now); err != nil {
//		return err
//	}
//
//	return nil
//}
//
//func updateWhisperFile(db *Whisper, tss [][]Point, now Timestamp) error {
//	if tss == nil {
//		return nil
//	}
//	for i := range db.Retentions {
//		err := db.UpdatePointsForArchive(i, tss[i], now)
//		if err != nil {
//			return err
//		}
//	}
//
//	if err := db.Flush(); err != nil {
//		return err
//	}
//	return nil
//}
