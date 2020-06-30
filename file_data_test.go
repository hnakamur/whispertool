package whispertool

import (
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

			m := Meta{
				aggregationMethod: Sum,
				xFilesFactor:      0,
			}
			d, err := NewFileData(m, retentions)
			if err != nil {
				t.Fatal(err)
			}

			rnd := rand.New(rand.NewSource(newRandSeed()))
			tsNow := tc.now
			tsUntil := tsNow
			const randMax = 100
			pointsList := randomPointsList(retentions, tsUntil, tsNow, rnd, randMax)
			if err := updateFileDataWithPointsList(d, pointsList, tsNow); err != nil {
				t.Fatal(err)
			}

			gotPointsList := make([][]Point, len(d.retentions))
			for retID := range d.retentions {
				gotPointsList[retID] = d.getAllRawUnsortedPoints(retID)
			}
			sortPointsListByTime(gotPointsList)

			wantPlDif, gotPlDif := PointsList(pointsList).Diff(gotPointsList)
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
			r := &d.retentions[retID]
			tsFrom := tsNow.Truncate(Minute).Add(-5 * Minute)
			tsUntil = tsFrom.Add(Minute)
			gotPoints, err := d.FetchFromArchive(retID, tsFrom, tsUntil, tsNow)
			if err != nil {
				t.Fatal(err)
			}
			wantPoints := filterPointsByTimeRange(r, pointsList[retID], tsFrom, tsUntil)
			sort.Stable(Points(wantPoints))
			wantPtsDif, gotPtsDif := Points(wantPoints).Diff(gotPoints)
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
			gotPoints, err = d.FetchFromArchive(retID, tsFrom, tsUntil, tsNow)
			if err != nil {
				t.Fatal(err)
			}
			wantPoints = filterPointsByTimeRange(r, pointsList[retID], tsFrom, tsUntil)
			sort.Stable(Points(wantPoints))
			wantPtsDif, gotPtsDif = Points(wantPoints).Diff(gotPoints)
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
