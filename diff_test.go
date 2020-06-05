package whispertool

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func formatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

func printTimeSeriesPoints(ts []*whisper.TimeSeriesPoint) {
	total := float64(0)
	for i, p := range ts {
		t := time.Unix(int64(ts[i].Time), 0)
		fmt.Printf("i=%d, time=%s, value=%g\n", i, formatTime(t), p.Value)
		total += p.Value
	}
}

func readTimeSeriesPointsForAllArchives(filename string, now time.Time) ([][]*whisper.TimeSeriesPoint, error) {
	oflag := os.O_RDONLY
	opts := &whisper.Options{OpenFileFlag: &oflag}
	db, err := whisper.OpenWithOptions(filename, opts)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	untilTime := int(now.Unix())
	retentions := db.Retentions()
	tss := make([][]*whisper.TimeSeriesPoint, len(retentions))
	for i, r := range retentions {
		fromTime := untilTime - r.MaxRetention()
		ts, err := db.Fetch(fromTime, untilTime)
		if err != nil {
			return nil, err
		}
		tss[i] = ts.PointPointers()
	}
	return tss, nil
}

func equalTimeSeriesPointPointers(ts1, ts2 []*whisper.TimeSeriesPoint) bool {
	if len(ts1) != len(ts2) {
		return false
	}
	for i, p1 := range ts1 {
		p2 := ts2[i]
		if !tsPointEqual(*p1, *p2, false) {
			return false
		}
	}
	return true
}

func equalTimeSeriesPointPointersForArchives(tss1, tss2 [][]*whisper.TimeSeriesPoint) bool {
	if len(tss1) != len(tss2) {
		return false
	}
	for i, ts1 := range tss1 {
		ts2 := tss2[i]
		if !equalTimeSeriesPointPointers(ts1, ts2) {
			return false
		}
	}
	return true
}

func deepCloneTimeSeriesPointPointers(ts []*whisper.TimeSeriesPoint) []*whisper.TimeSeriesPoint {
	ts2 := make([]*whisper.TimeSeriesPoint, len(ts))
	for i, p := range ts {
		ts2[i] = &whisper.TimeSeriesPoint{
			Time:  p.Time,
			Value: p.Value,
		}
	}
	return ts2
}

func deepCloneTimeSeriesPointPointersForArchives(tss [][]*whisper.TimeSeriesPoint) [][]*whisper.TimeSeriesPoint {
	tss2 := make([][]*whisper.TimeSeriesPoint, len(tss))
	for i, ts := range tss {
		tss2[i] = deepCloneTimeSeriesPointPointers(ts)
	}
	return tss2
}

func emptyRandomPointsInTimeSeriesPoints(ts []*whisper.TimeSeriesPoint, rnd *rand.Rand) {
	for _, p := range ts {
		if rnd.Intn(100) < 20 {
			p.Value = math.NaN()
		}
	}
}

func emptyRandomPointsInTimeSeriesPointsForAllArchives(tss [][]*whisper.TimeSeriesPoint, rnd *rand.Rand) {
	for _, ts := range tss {
		emptyRandomPointsInTimeSeriesPoints(ts, rnd)
	}
}

func TestDiff(t *testing.T) {
	srcFilename := "src.wsp"
	retentionDefs := "1m:1h,1h:1d,1d:30d"
	retentions, err := whisper.ParseRetentionDefs(retentionDefs)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("len(retentions)=%d\n", len(retentions))
	os.Remove(srcFilename) // don't care if error occur
	srcDB, err := whisper.Create(srcFilename, retentions, whisper.Sum, 0)
	if err != nil {
		t.Fatal(err)
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	now := time.Now()
	until := now
	rndMaxForHightestArchive := int(100)
	tss := randomTimeSeriesPointsForArchives(retentions, until, now, rnd, rndMaxForHightestArchive)
	for i, r := range retentions {
		fmt.Printf("retention %d ===============\n", i)
		printTimeSeriesPoints(tss[i])
		err := srcDB.UpdateManyForArchive(tss[i], r.MaxRetention())
		if err != nil {
			t.Fatal(err)
		}
	}
	fmt.Printf("srcDB=%+v\n", srcDB)
	srcDB.Close()

	tss2, err := readTimeSeriesPointsForAllArchives(srcFilename, now)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("tss2 is equal to tss: %v\n", equalTimeSeriesPointPointersForArchives(tss, tss2))
	for i, ts2 := range tss2 {
		fmt.Printf("tss2 retention %d ===============\n", i)
		printTimeSeriesPoints(ts2)
	}

	emptyRandomPointsInTimeSeriesPointsForAllArchives(tss2, rnd)
	for i, ts2 := range tss2 {
		fmt.Printf("tss2 with empty points retention %d ===============\n", i)
		printTimeSeriesPoints(ts2)
	}

	if !timeEqualMultiTimeSeriesPointsPointers(tss, tss2) {
		t.Fatal("time unmatch between tss and tss2")
	}

	ignoreSrcEmpty := true
	iss := valueDiffIndexesMultiTimeSeriesPointsPointers(tss, tss2, ignoreSrcEmpty)
	for i, is := range iss {
		fmt.Printf("diff from tss to tss2 in retention %s ===============\n", retentions[i])
		ts := tss[i]
		ts2 := tss2[i]
		for _, j := range is {
			fmt.Printf("{Time:%s SrcVal:%g DestVal:%g}\n",
				formatUnixTime(ts[j].Time), ts[j].Value, ts2[j].Value)
		}
	}

	iss = valueDiffIndexesMultiTimeSeriesPointsPointers(tss2, tss, ignoreSrcEmpty)
	for i, is := range iss {
		fmt.Printf("diff from tss2 to tss in retention %s ===============\n", retentions[i])
		ts := tss[i]
		ts2 := tss2[i]
		for _, j := range is {
			fmt.Printf("{Time:%s SrcVal:%g DestVal:%g}\n",
				formatUnixTime(ts[j].Time), ts[j].Value, ts2[j].Value)
		}
	}

	//defer os.Remove(srcFilename)
}
