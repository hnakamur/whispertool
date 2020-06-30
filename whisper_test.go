package whispertool

import (
	crand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/willf/bitset"
)

func TestDirtyPageRanges(t *testing.T) {
	b := bitset.New(9)
	b.Set(0).Set(1).Set(3).Set(6).Set(7).Set(8)
	got := dirtyPageRanges(b)
	want := []pageRange{
		{start: 0, end: 2},
		{start: 3, end: 4},
		{start: 6, end: 9},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ranges unmatch, got=%+v, want=%+v", got, want)
	}
}

func TestRetention_pointIndex(t *testing.T) {
	r := &Retention{
		secondsPerPoint: Second,
		numberOfPoints:  5,
	}

	baseInterval := TimestampFromStdTime(time.Date(2020, 6, 28, 9, 51, 0, 0, time.UTC))
	testCases := []struct {
		interval Timestamp
		want     int
	}{
		// same as base interval
		{
			interval: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 51, 0, 0, time.UTC)),
			want:     0,
		},

		// newer timestamps

		{
			interval: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 51, 1, 0, time.UTC)),
			want:     1,
		},
		{
			interval: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 51, 2, 0, time.UTC)),
			want:     2,
		},
		{
			interval: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 51, 3, 0, time.UTC)),
			want:     3,
		},
		{
			interval: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 51, 4, 0, time.UTC)),
			want:     4,
		},
		{
			interval: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 51, 5, 0, time.UTC)),
			want:     0,
		},

		// older timestamps

		{
			interval: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 59, 0, time.UTC)),
			want:     4,
		},
		{
			interval: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 58, 0, time.UTC)),
			want:     3,
		},
		{
			interval: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 57, 0, time.UTC)),
			want:     2,
		},
		{
			interval: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 56, 0, time.UTC)),
			want:     1,
		},
		{
			interval: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 55, 0, time.UTC)),
			want:     0,
		},

		// unaligned timestamps

		{
			interval: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 51, 1, 999999999, time.UTC)),
			want:     1,
		},
	}
	for _, tc := range testCases {
		got := r.pointIndex(baseInterval, tc.interval)
		if got != tc.want {
			t.Errorf("index unmatch for interval=%s, got=%d, want=%d", tc.interval, got, tc.want)
		}
	}
}

func TestFileDataWriteReadHigestRetention(t *testing.T) {
	retentionDefs := "1m:2h,1h:2d,1d:30d"

	testCases := []struct {
		now Timestamp
	}{
		{now: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 0, 0, time.UTC))}, // aligned to retention
		{now: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 1, 0, time.UTC))}, // unaligned to retention
	}
	for _, tc := range testCases {
		tc := tc
		t.Run("now_"+tc.now.String(), func(t *testing.T) {
			t.Parallel()
			retentions, err := ParseRetentions(retentionDefs)
			if err != nil {
				t.Fatal(err)
			}

			db, err := Create("", retentions, Sum, 0, WithInMemory())
			if err != nil {
				t.Fatal(err)
			}

			rnd := rand.New(rand.NewSource(newRandSeed()))
			tsNow := tc.now
			tsUntil := tsNow
			const randMax = 100
			pointsList := randomPointsList(retentions, rnd, randMax, tsUntil, tsNow)
			if err := updateFileDataWithPointsList(db, pointsList, tsNow); err != nil {
				t.Fatal(err)
			}

			gotPointsList := make([]Points, len(db.Retentions()))
			for retID := range db.Retentions() {
				gotPointsList[retID] = db.GetAllRawUnsortedPoints(retID)
			}
			sortPointsListByTime(gotPointsList)

			wantPlDif, gotPlDif := diffPointsList(pointsList, gotPointsList)
			for retID, gotPtsDif := range gotPlDif {
				wantPtsDif := wantPlDif[retID]
				if len(gotPtsDif) != len(wantPtsDif) {
					t.Errorf("points count unmatch for retention %d, got=%d, want=%d", retID, len(gotPtsDif), len(wantPtsDif))
				}
				for i, gotPt := range gotPtsDif {
					wantPt := wantPtsDif[i]
					t.Errorf("point unmatch for retention %d, got=%s, want=%s", retID, gotPt, wantPt)
				}
			}

			retID := 0
			r := &db.Retentions()[retID]
			tsFrom := tsNow.Truncate(Minute).Add(-5 * Minute)
			tsUntil = tsFrom.Add(Minute)
			gotPoints, err := db.FetchFromArchive(retID, tsFrom, tsUntil, tsNow)
			if err != nil {
				t.Fatal(err)
			}
			wantPoints := filterPointsByTimeRange(r, pointsList[retID], tsFrom, tsUntil)
			sort.Stable(wantPoints)
			wantPtsDif, gotPtsDif := wantPoints.Diff(gotPoints)
			if len(gotPtsDif) != len(wantPtsDif) {
				t.Errorf("points count unmatch for retention %d, now=%s, from=%s, until=%s, got=%d, want=%d", retID, tsNow, tsFrom, tsUntil, len(gotPtsDif), len(wantPtsDif))
			}
			for i, gotPt := range gotPtsDif {
				wantPt := wantPtsDif[i]
				t.Errorf("point unmatch for retention %d, now=%s, from=%s, until=%s, got=%s, want=%s", retID, tsNow, tsFrom, tsUntil, gotPt, wantPt)
			}
			t.Logf("now=%s, from=%s, until=%s, gotPoints=%v", tsNow, tsFrom, tsUntil, gotPoints)

			tsFrom = tsNow.Add(-5 * Minute)
			tsUntil = tsFrom.Add(Minute)
			gotPoints, err = db.FetchFromArchive(retID, tsFrom, tsUntil, tsNow)
			if err != nil {
				t.Fatal(err)
			}
			wantPoints = filterPointsByTimeRange(r, pointsList[retID], tsFrom, tsUntil)
			sort.Stable(wantPoints)
			wantPtsDif, gotPtsDif = wantPoints.Diff(gotPoints)
			if len(gotPtsDif) != len(wantPtsDif) {
				t.Errorf("points count unmatch for retention %d, now=%s, from=%s, until=%s, got=%d, want=%d", retID, tsNow, tsFrom, tsUntil, len(gotPtsDif), len(wantPtsDif))
			}
			for i, gotPt := range gotPtsDif {
				wantPt := wantPtsDif[i]
				t.Errorf("point unmatch for retention %d, now=%s, from=%s, until=%s, got=%s, want=%s", retID, tsNow, tsFrom, tsUntil, gotPt, wantPt)
			}
			t.Logf("now=%s, from=%s, until=%s, gotPoints=%v", tsNow, tsFrom, tsUntil, gotPoints)
		})
	}
}

func newRandSeed() int64 {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return time.Now().UnixNano()
	}
	return int64(binary.BigEndian.Uint64(b[:]))
}

func randomPointsList(retentions []Retention, rnd *rand.Rand, rndMaxForHightestArchive int, until, now Timestamp) []Points {
	pointsList := make([]Points, len(retentions))
	var highRet *Retention
	var highRndMax int
	var highPts []Point
	for i := range retentions {
		r := &retentions[i]
		rndMax := rndMaxForHightestArchive * int(r.SecondsPerPoint()) / int(retentions[0].SecondsPerPoint())
		pointsList[i] = randomPoints(r, highRet, highPts, rnd, rndMax, highRndMax, until, now)

		highRndMax = rndMax
		highPts = pointsList[i]
		highRet = r
	}
	return pointsList
}

func randomPoints(r, highRet *Retention, highPts []Point, rnd *rand.Rand, rndMax, highRndMax int, until, now Timestamp) []Point {
	// adjust now and until for this archive
	step := r.SecondsPerPoint()
	thisNow := now.Truncate(step)
	thisUntil := until.Truncate(step)

	var thisHighStartTime Timestamp
	if highPts != nil {
		highStartTime := highPts[0].Time
		if highStartTime < thisUntil {
			thisHighStartTime = highStartTime.Truncate(step)
		}
	}

	n := int((r.MaxRetention() - thisNow.Sub(thisUntil)) / r.SecondsPerPoint())
	points := make([]Point, n)
	for i := 0; i < n; i++ {
		t := thisUntil.Add(-Duration(n-1-i) * step * Second)
		var v Value
		if thisHighStartTime == 0 || t < thisHighStartTime {
			v = Value(rnd.Intn(rndMax + 1))
		} else {
			v = randomValWithHighSum(t, rnd, highRndMax, r, highRet, highPts)
		}
		points[i] = Point{
			Time:  t,
			Value: v,
		}
	}
	return points
}

func randomValWithHighSum(t Timestamp, rnd *rand.Rand, highRndMax int, r, highRet *Retention, highPts []Point) Value {
	step := r.SecondsPerPoint()

	v := Value(0)
	for _, hp := range highPts {
		thisHighTime := hp.Time.Truncate(step)
		if thisHighTime < t {
			continue
		}
		if thisHighTime > t {
			break
		}
		v += hp.Value
	}

	if len(highPts) == 0 {
		return v
	}
	highStartTime := highPts[0].Time
	if t >= highStartTime {
		return v
	}
	n := int(highStartTime.Sub(t) / Second / highRet.SecondsPerPoint())
	v2 := Value(n * rnd.Intn(highRndMax+1))
	return v + v2
}

func updateFileDataWithPointsList(db *Whisper, pointsList []Points, now Timestamp) error {
	for retID := range db.Retentions() {
		if err := db.UpdatePointsForArchive(retID, pointsList[retID], now); err != nil {
			return err
		}
	}
	return nil
}

func sortPointsListByTime(pointsList []Points) {
	for _, points := range pointsList {
		sort.Stable(points)
	}
}

func filterPointsByTimeRange(r *Retention, points []Point, from, until Timestamp) Points {
	if until == from {
		until = until.Add(r.SecondsPerPoint())
	}
	var points2 []Point
	for _, p := range points {
		if (from != 0 && p.Time <= from) || p.Time > until {
			continue
		}
		points2 = append(points2, p)
	}
	return points2
}

func diffPointsList(pl, ql []Points) ([]Points, []Points) {
	if len(pl) != len(ql) {
		return pl, ql
	}

	pl2 := make([]Points, len(pl))
	ql2 := make([]Points, len(ql))
	for i, pp := range pl {
		pl2[i], ql2[i] = pp.Diff(ql[i])
	}
	return pl2, ql2
}
