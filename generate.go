package whispertool

import (
	crand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func Generate(dest string, retentionDefs string, fill bool, randMax int) error {
	retentions, err := whisper.ParseRetentionDefs(retentionDefs)
	if err != nil {
		return err
	}

	d := &whisperFileData{
		retentions:   retentionsToRetentionSlice(retentions),
		aggMethod:    "Sum",
		xFilesFactor: 0,
	}

	if fill {
		rnd := rand.New(rand.NewSource(newRandSeed()))
		now := time.Now()
		until := now
		d.tss = randomTimeSeriesPointsForArchives(retentions, until, now,
			rnd, randMax)
	}

	return createWhisperFile(dest, d)
}

func newRandSeed() int64 {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return time.Now().UnixNano()
	}
	return int64(binary.BigEndian.Uint64(b[:]))
}

func retentionsToRetentionSlice(retentions whisper.Retentions) []whisper.Retention {
	retentions2 := make([]whisper.Retention, len(retentions))
	for i, r := range retentions {
		retentions2[i] = whisper.NewRetention(
			r.SecondsPerPoint(),
			r.NumberOfPoints())
	}
	return retentions2
}

func retentionSliceToRetentions(retentions []whisper.Retention) whisper.Retentions {
	retentions2 := make([]*whisper.Retention, len(retentions))
	for i := range retentions {
		retentions2[i] = &retentions[i]
	}
	return retentions2
}

func alignUnixTime(t int64, secondsPerPoint int) int64 {
	return t - t%int64(secondsPerPoint)
}

func alignTime(t time.Time, secondsPerPoint int) time.Time {
	return time.Unix(alignUnixTime(t.Unix(), secondsPerPoint), 0)
}

func randomValWithHighSum(t time.Time, rnd *rand.Rand, highRndMax int, r, highRet *whisper.Retention, highPts []*whisper.TimeSeriesPoint) float64 {
	step := r.SecondsPerPoint()

	v := float64(0)
	for _, hp := range highPts {
		highTime := time.Unix(int64(hp.Time), 0)
		thisHighTime := alignTime(highTime, step)
		if thisHighTime.Before(t) {
			continue
		}
		if thisHighTime.After(t) {
			break
		}
		v += hp.Value
	}

	if len(highPts) == 0 {
		return v
	}
	highStartTime := time.Unix(int64(highPts[0].Time), 0)
	if !t.Before(highStartTime) {
		return v
	}
	n := int(highStartTime.Sub(t)/time.Second) / highRet.SecondsPerPoint()
	v2 := float64(n * rnd.Intn(highRndMax))
	return v + v2
}

func randomTimeSeriesPoints(until, now time.Time, r, highRet *whisper.Retention, rnd *rand.Rand, rndMax, highRndMax int, highPts []*whisper.TimeSeriesPoint) []*whisper.TimeSeriesPoint {
	// adjust now and until for this archive
	step := r.SecondsPerPoint()
	thisNow := alignTime(now, step)
	thisUntil := alignTime(until, step)

	var thisHighStartTime time.Time
	if highPts != nil {
		highStartTime := time.Unix(int64(highPts[0].Time), 0)
		if highStartTime.Before(thisUntil) {
			thisHighStartTime = alignTime(highStartTime, step)
		}
	}

	n := (r.MaxRetention() - int(thisNow.Sub(thisUntil)/time.Second)) / r.SecondsPerPoint()
	ts := make([]*whisper.TimeSeriesPoint, n)
	for i := 0; i < n; i++ {
		t := thisUntil.Add(-time.Duration((n-1-i)*step) * time.Second)
		var v float64
		if thisHighStartTime.IsZero() || t.Before(thisHighStartTime) {
			v = float64(rnd.Intn(rndMax))
		} else {
			v = randomValWithHighSum(t, rnd, highRndMax, r, highRet, highPts)
		}
		ts[i] = &whisper.TimeSeriesPoint{
			Time:  int(t.Unix()),
			Value: v,
		}
	}
	return ts
}

func randomTimeSeriesPointsForArchives(retentions []*whisper.Retention, until, now time.Time, rnd *rand.Rand, rndMaxForHightestArchive int) [][]*whisper.TimeSeriesPoint {
	tss := make([][]*whisper.TimeSeriesPoint, len(retentions))
	var highRet *whisper.Retention
	var highRndMax int
	var highPts []*whisper.TimeSeriesPoint
	for i, r := range retentions {
		rndMax := rndMaxForHightestArchive * r.SecondsPerPoint() / retentions[0].SecondsPerPoint()
		tss[i] = randomTimeSeriesPoints(until, now, r, highRet, rnd, rndMax, highRndMax, highPts)

		highRndMax = rndMax
		highPts = tss[i]
		highRet = r
	}
	return tss
}

func createWhisperFile(filename string, d *whisperFileData) error {
	aggMethod, err := stringToAggregationMethod(d.aggMethod)
	if err != nil {
		return err
	}

	db, err := whisper.Create(filename,
		retentionSliceToRetentions(d.retentions),
		aggMethod,
		d.xFilesFactor)
	if err != nil {
		return err
	}
	defer db.Close()

	return updateWhisperFile(db, d.tss)
}

func updateWhisperFile(db *whisper.Whisper, tss [][]*whisper.TimeSeriesPoint) error {
	if tss == nil {
		return nil
	}
	for i, r := range db.Retentions() {
		err := db.UpdateManyForArchive(tss[i], r.MaxRetention())
		if err != nil {
			return err
		}
	}
	return nil
}
