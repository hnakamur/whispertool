package whispertool

import (
	"math/rand"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func Generate(dest string, retentionDefs string, randMax int) error {
	retentions, err := whisper.ParseRetentionDefs(retentionDefs)
	if err != nil {
		return err
	}
	srcDB, err := whisper.Create(dest, retentions, whisper.Sum, 0)
	if err != nil {
		return err
	}
	defer srcDB.Close()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	now := time.Now()
	until := now
	tss := randomTimeSeriesPointsForArchives(retentions, until, now,
		rnd, randMax)
	for i, r := range retentions {
		err := srcDB.UpdateManyForArchive(tss[i], r.MaxRetention())
		if err != nil {
			return err
		}
	}
	return nil
}

func alignTime(t time.Time, secondsPerPoint int) time.Time {
	return time.Unix(t.Unix()-t.Unix()%int64(secondsPerPoint), 0)
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
