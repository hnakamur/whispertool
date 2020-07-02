package whispertool

import (
	crand "crypto/rand"
	"encoding/binary"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
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
			ts, err := db.FetchFromArchive(retID, tsFrom, tsUntil, tsNow)
			if err != nil {
				t.Fatal(err)
			}
			gotPoints := ts.Points()
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

			tsFrom = tsNow.Add(-5 * Minute)
			tsUntil = tsFrom.Add(Minute)
			ts, err = db.FetchFromArchive(retID, tsFrom, tsUntil, tsNow)
			if err != nil {
				t.Fatal(err)
			}
			gotPoints = ts.Points()
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

func fileExists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

func setUpCreate(t *testing.T) (path string, archiveList Retentions) {
	file, err := ioutil.TempFile("", "whisper-testing-*.wsp")
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	t.Cleanup(func() {
		os.Remove(file.Name())
	})
	archiveList = Retentions{
		{secondsPerPoint: 1, numberOfPoints: 300},
		{secondsPerPoint: 60, numberOfPoints: 30},
		{secondsPerPoint: 300, numberOfPoints: 12},
	}
	return file.Name(), archiveList
}

func TestCreateCreatesFile(t *testing.T) {
	path, retentions := setUpCreate(t)
	expected := []byte{
		// Metadata
		0x00, 0x00, 0x00, 0x01, // Aggregation type
		0x00, 0x00, 0x0e, 0x10, // Max retention
		0x3f, 0x00, 0x00, 0x00, // xFilesFactor
		0x00, 0x00, 0x00, 0x03, // Retention count
		// Archive Info
		// Retention 1 (1, 300)
		0x00, 0x00, 0x00, 0x34, // offset
		0x00, 0x00, 0x00, 0x01, // secondsPerPoint
		0x00, 0x00, 0x01, 0x2c, // numberOfPoints
		// Retention 2 (60, 30)
		0x00, 0x00, 0x0e, 0x44, // offset
		0x00, 0x00, 0x00, 0x3c, // secondsPerPoint
		0x00, 0x00, 0x00, 0x1e, // numberOfPoints
		// Retention 3 (300, 12)
		0x00, 0x00, 0x0f, 0xac, // offset
		0x00, 0x00, 0x01, 0x2c, // secondsPerPoint
		0x00, 0x00, 0x00, 0x0c} // numberOfPoints
	os.Remove(path)
	whisper, err := Create(path, retentions, Average, 0.5)
	if err != nil {
		t.Fatalf("failed to create whisper file: %v", err)
	}
	if whisper.meta.aggregationMethod != Average {
		t.Errorf("Unexpected aggregationMethod %v, expected %v", whisper.meta.aggregationMethod, Average)
	}
	if whisper.meta.maxRetention != 3600 {
		t.Errorf("Unexpected maxRetention %v, expected 3600", whisper.meta.maxRetention)
	}
	if whisper.meta.xFilesFactor != 0.5 {
		t.Errorf("Unexpected xFilesFactor %v, expected 0.5", whisper.meta.xFilesFactor)
	}
	if len(whisper.retentions) != 3 {
		t.Errorf("Unexpected archive count %v, expected 3", len(whisper.retentions))
	}
	if err := whisper.Sync(); err != nil {
		t.Fatalf("failed to sync whisper file: %v", err)
	}
	if err := whisper.Close(); err != nil {
		t.Fatalf("failed to close whisper file: %v", err)
	}
	if !fileExists(path) {
		t.Fatalf("File does not exist after create")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Failed to open whisper file")
	}
	contents := make([]byte, len(expected))
	if _, err := io.ReadFull(file, contents); err != nil {
		t.Fatalf("failed to read whisper file: %v", err)
	}

	for i := 0; i < len(contents); i++ {
		if expected[i] != contents[i] {
			// Show what is being written
			// for i := 0; i < 13; i++ {
			// 	for j := 0; j < 4; j ++ {
			// 		fmt.Printf("  %02x", contents[(i*4)+j])
			// 	}
			// 	fmt.Print("\n")
			// }
			t.Errorf("File is incorrect at character %v, expected %x got %x", i, expected[i], contents[i])
		}
	}

	// test size
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 4156 {
		t.Errorf("File size is incorrect, expected %v got %v", 4156, info.Size())
	}
}

func TestCreateFileAlreadyExists(t *testing.T) {
	path, retentions := setUpCreate(t)
	_, err := Create(path, retentions, Average, 0.5)
	if err == nil {
		t.Fatalf("Existing file should cause create to fail.")
	}
}

func TestCreateFileInvalidRetentionDefs(t *testing.T) {
	path, retentions := setUpCreate(t)
	// Add a small retention def on the end
	retentions = append(retentions, Retention{secondsPerPoint: 1, numberOfPoints: 200})
	_, err := Create(path, retentions, Average, 0.5)
	if err == nil {
		t.Fatalf("Invalid retention definitions should cause create to fail.")
	}
}

func TestOpenFile(t *testing.T) {
	path, retentions := setUpCreate(t)
	os.Remove(path)
	whisper1, err := Create(path, retentions, Average, 0.5)
	if err != nil {
		t.Errorf("Failed to create: %v", err)
	}

	// write some points
	now := TimestampFromStdTime(time.Now())
	for i := 0; i < 2; i++ {
		if err := whisper1.Update(now.Add(-Duration(i)*Second), 100); err != nil {
			t.Fatalf("failed to update a point in database: %v", err)
		}
	}
	if err := whisper1.Sync(); err != nil {
		t.Fatalf("failed to sync whisper1: %v", err)
	}

	whisper2, err := Open(path)
	if err != nil {
		t.Fatalf("Failed to open whisper file: %v", err)
	}
	if whisper1.AggregationMethod() != whisper2.AggregationMethod() {
		t.Errorf("AggregationMethod() did not match, expected %v, received %v", whisper1.AggregationMethod(), whisper2.AggregationMethod())
	}
	if whisper1.MaxRetention() != whisper2.MaxRetention() {
		t.Errorf("MaxRetention() did not match, expected %v, received %v", whisper1.MaxRetention(), whisper2.MaxRetention())
	}
	if whisper1.XFilesFactor() != whisper2.XFilesFactor() {
		t.Errorf("XFilesFactor() did not match, expected %v, received %v", whisper1.XFilesFactor(), whisper2.XFilesFactor())
	}
	if len(whisper1.Retentions()) != len(whisper2.Retentions()) {
		t.Errorf("archive count does not match, expected %v, received %v", len(whisper1.Retentions()), len(whisper2.Retentions()))
	}
	for i := range whisper1.Retentions() {
		if whisper1.Retentions()[i].offset != whisper2.Retentions()[i].offset {
			t.Errorf("archive mismatch offset at %v [%v, %v]", i, whisper1.Retentions()[i].offset, whisper2.Retentions()[i].offset)
		}
		if whisper1.Retentions()[i].secondsPerPoint != whisper2.Retentions()[i].secondsPerPoint {
			t.Errorf("secondsPerPoint mismatch offset at %v [%v, %v]", i, whisper1.Retentions()[i].secondsPerPoint, whisper2.Retentions()[i].secondsPerPoint)
		}
		if whisper1.Retentions()[i].numberOfPoints != whisper2.Retentions()[i].numberOfPoints {
			t.Errorf("numberOfPoints mismatch offset at %v [%v, %v]", i, whisper1.Retentions()[i].numberOfPoints, whisper2.Retentions()[i].numberOfPoints)
		}

	}

	result1, err := whisper1.Fetch(now-3, now)
	if err != nil {
		t.Fatalf("Error retrieving result from created whisper: %v", err)
	}
	result2, err := whisper2.Fetch(now-3, now)
	if err != nil {
		t.Fatalf("Error retrieving result from opened whisper: %v", err)
	}

	pts1 := result1.Points()
	pts2 := result2.Points()
	if !pts1.Equal(pts2) {
		ptsDiff1, ptsDiff2 := pts1.Diff(pts2)
		t.Errorf("Results do not match")
		for i, pt1 := range ptsDiff1 {
			pt2 := ptsDiff2[i]
			t.Logf("t1:%s\tt2:%s\tv1:%s\tv2:%s\n", pt1.Time, pt2.Time, pt1.Value, pt2.Value)
		}
	}
}

func TestCreateUpdateFetch(t *testing.T) {
	var pts Points
	pts = testCreateUpdateFetch(t, Average, 0.5, 3500, 3500, 1000, 300, 0.5, 0.2)
	assertFloatAlmostEqual(t, float64(pts[1].Value), 150.1, 58.0)
	assertFloatAlmostEqual(t, float64(pts[2].Value), 210.75, 28.95)

	pts = testCreateUpdateFetch(t, Sum, 0.5, 600, 600, 500, 60, 0.5, 0.2)
	assertFloatAlmostEqual(t, float64(pts[0].Value), 18.35, 5.95)
	assertFloatAlmostEqual(t, float64(pts[1].Value), 30.35, 5.95)
	// 4 is a crazy one because it fluctuates between 60 and ~4k
	assertFloatAlmostEqual(t, float64(pts[5].Value), 4356.05, 500.0)

	pts = testCreateUpdateFetch(t, Last, 0.5, 300, 300, 200, 1, 0.5, 0.2)
	assertFloatAlmostEqual(t, float64(pts[0].Value), 0.7, 0.001)
	assertFloatAlmostEqual(t, float64(pts[10].Value), 2.7, 0.001)
	assertFloatAlmostEqual(t, float64(pts[20].Value), 4.7, 0.001)

}

/*
  Test the full cycle of creating a whisper file, adding some
  data points to it and then fetching a time series.
*/
func testCreateUpdateFetch(t *testing.T, aggregationMethod AggregationMethod, xFilesFactor float32, secondsAgo, fromAgo, fetchLength, step Duration, currentValue, increment Value) Points {
	var whisper *Whisper
	var err error
	path, archiveList := setUpCreate(t)
	os.Remove(path)
	whisper, err = Create(path, archiveList, aggregationMethod, xFilesFactor)
	if err != nil {
		t.Fatalf("Failed create: %v", err)
	}
	defer whisper.Close()

	now := TimestampFromStdTime(time.Now())

	for i := Duration(0); i < secondsAgo; i++ {
		err = whisper.Update(now.Add(-secondsAgo+i), currentValue)
		if err != nil {
			t.Fatalf("Unexpected error for %v: %v", i, err)
		}
		currentValue += increment
	}
	if err := whisper.Sync(); err != nil {
		t.Fatalf("failed to sync whisper: %v", err)
	}

	fromTime := now.Add(-fromAgo)
	untilTime := fromTime.Add(fetchLength)

	ts, err := whisper.Fetch(fromTime, untilTime)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !validTimestamp(ts.FromTime(), fromTime, step) {
		t.Fatalf("Invalid fromTime [%v/%v], expected %v, received %v", secondsAgo, fromAgo, fromTime, ts.FromTime())
	}
	if !validTimestamp(ts.UntilTime(), untilTime, step) {
		t.Fatalf("Invalid untilTime [%v/%v], expected %v, received %v", secondsAgo, fromAgo, untilTime, ts.UntilTime())
	}
	if ts.Step() != step {
		t.Fatalf("Invalid step [%v/%v], expected %v, received %v", secondsAgo, fromAgo, step, ts.Step())
	}

	return ts.Points()
}

func validTimestamp(value, stamp Timestamp, step Duration) bool {
	return value == nearestStep(stamp, step) || value == nearestStep(stamp, step).Add(step)
}
func nearestStep(stamp Timestamp, step Duration) Timestamp {
	return stamp.Add(-(Duration(stamp) % step) + step)
}

func assertFloatAlmostEqual(t *testing.T, received, expected, slop float64) {
	if math.Abs(expected-received) > slop {
		t.Fatalf("Expected %v to be within %v of %v", expected, slop, received)
	}
}

func assertFloatEqual(t *testing.T, received, expected float64) {
	if math.Abs(expected-received) > 0.00001 {
		t.Fatalf("Expected %v, received %v", expected, received)
	}
}

func test_aggregate(t *testing.T, method AggregationMethod, expected Value) {
	received := aggregate(method, []Value{1.0, 2.0, 3.0, 5.0, 4.0})
	if expected != received {
		t.Fatalf("Expected %v, received %v", expected, received)
	}
}
func Test_aggregateAverage(t *testing.T) {
	test_aggregate(t, Average, 3.0)
}

func Test_aggregateSum(t *testing.T) {
	test_aggregate(t, Sum, 15.0)
}

func Test_aggregateFirst(t *testing.T) {
	test_aggregate(t, First, 1.0)
}

func Test_aggregateLast(t *testing.T) {
	test_aggregate(t, Last, 4.0)
}

func Test_aggregateMax(t *testing.T) {
	test_aggregate(t, Max, 5.0)
}

func Test_aggregateMin(t *testing.T) {
	test_aggregate(t, Min, 1.0)
}
