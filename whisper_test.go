package whispertool

import (
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestArchiveInfo_pointIndex(t *testing.T) {
	r := &ArchiveInfo{
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

func TestFileDataWriteReadHigestArchive(t *testing.T) {
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

			file, err := ioutil.TempFile("", "whispertool-test-*.wsp")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				os.Remove(file.Name())
			})

			archiveInfoList, err := ParseArchiveInfoList(retentionDefs)
			if err != nil {
				t.Fatal(err)
			}

			db, err := Create(file.Name(), archiveInfoList, Sum, 0, WithOpenFileFlag(os.O_RDWR))
			if err != nil {
				t.Fatal(err)
			}

			rnd := rand.New(rand.NewSource(newRandSeed()))
			tsNow := tc.now
			tsUntil := tsNow
			const randMax = 100
			pointsList := randomPointsList(archiveInfoList, rnd, randMax, tsUntil, tsNow)
			if err := updateFileDataWithPointsList(db, pointsList, tsNow); err != nil {
				t.Fatal(err)
			}

			gotPointsList := make([]Points, len(db.ArchiveInfoList()))
			for retID := range db.ArchiveInfoList() {
				gotPointsList[retID], err = db.GetAllRawUnsortedPoints(retID)
				if err != nil {
					t.Fatal(err)
				}
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
			r := &db.ArchiveInfoList()[retID]
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

func randomPointsList(retentions []ArchiveInfo, rnd *rand.Rand, rndMaxForHightestArchive int, until, now Timestamp) []Points {
	pointsList := make([]Points, len(retentions))
	var highRet *ArchiveInfo
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

func randomPoints(r, highRet *ArchiveInfo, highPts []Point, rnd *rand.Rand, rndMax, highRndMax int, until, now Timestamp) []Point {
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

func randomValWithHighSum(t Timestamp, rnd *rand.Rand, highRndMax int, r, highRet *ArchiveInfo, highPts []Point) Value {
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
	for retID := range db.ArchiveInfoList() {
		if err := db.UpdatePointsForArchive(pointsList[retID], retID, now); err != nil {
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

func filterPointsByTimeRange(r *ArchiveInfo, points []Point, from, until Timestamp) Points {
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

func setUpCreate(t *testing.T) (path string, archiveList ArchiveInfoList) {
	file, err := ioutil.TempFile("", "whisper-testing-*.wsp")
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	t.Cleanup(func() {
		os.Remove(file.Name())
	})
	archiveList = ArchiveInfoList{
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
	whisper, err := Create(path, retentions, Average, 0.5, WithOpenFileFlag(os.O_RDWR))
	if err != nil {
		t.Fatalf("failed to create whisper file: %v", err)
	}
	if whisper.AggregationMethod() != Average {
		t.Errorf("Unexpected aggregationMethod %v, expected %v", whisper.AggregationMethod(), Average)
	}
	if whisper.MaxRetention() != 3600 {
		t.Errorf("Unexpected maxRetention %v, expected 3600", whisper.MaxRetention())
	}
	if whisper.XFilesFactor() != 0.5 {
		t.Errorf("Unexpected xFilesFactor %v, expected 0.5", whisper.XFilesFactor())
	}
	if len(whisper.ArchiveInfoList()) != 3 {
		t.Errorf("Unexpected archive count %v, expected 3", len(whisper.ArchiveInfoList()))
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
	retentions = append(retentions, ArchiveInfo{secondsPerPoint: 1, numberOfPoints: 200})
	_, err := Create(path, retentions, Average, 0.5, WithOpenFileFlag(os.O_RDWR))
	if err == nil {
		t.Fatalf("Invalid retention definitions should cause create to fail.")
	}
}

func TestOpenFile(t *testing.T) {
	path, retentions := setUpCreate(t)
	whisper1, err := Create(path, retentions, Average, 0.5, WithOpenFileFlag(os.O_RDWR), WithoutFlock())
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

	whisper2, err := Open(path, WithoutFlock())
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
	if len(whisper1.ArchiveInfoList()) != len(whisper2.ArchiveInfoList()) {
		t.Errorf("archive count does not match, expected %v, received %v", len(whisper1.ArchiveInfoList()), len(whisper2.ArchiveInfoList()))
	}
	for i := range whisper1.ArchiveInfoList() {
		if whisper1.ArchiveInfoList()[i].offset != whisper2.ArchiveInfoList()[i].offset {
			t.Errorf("archive mismatch offset at %v [%v, %v]", i, whisper1.ArchiveInfoList()[i].offset, whisper2.ArchiveInfoList()[i].offset)
		}
		if whisper1.ArchiveInfoList()[i].secondsPerPoint != whisper2.ArchiveInfoList()[i].secondsPerPoint {
			t.Errorf("secondsPerPoint mismatch offset at %v [%v, %v]", i, whisper1.ArchiveInfoList()[i].secondsPerPoint, whisper2.ArchiveInfoList()[i].secondsPerPoint)
		}
		if whisper1.ArchiveInfoList()[i].numberOfPoints != whisper2.ArchiveInfoList()[i].numberOfPoints {
			t.Errorf("numberOfPoints mismatch offset at %v [%v, %v]", i, whisper1.ArchiveInfoList()[i].numberOfPoints, whisper2.ArchiveInfoList()[i].numberOfPoints)
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
	var ts *TimeSeries
	ts = testCreateUpdateFetch(t, Average, 0.5, 3500, 3500, 1000, 300, 0.5, 0.2)
	assertFloatAlmostEqual(t, float64(ts.Points()[1].Value), 150.1, 58.0)
	assertFloatAlmostEqual(t, float64(ts.Points()[2].Value), 210.75, 28.95)
	ts = testCreateUpdateFetch(t, Sum, 0.5, 600, 600, 500, 60, 0.5, 0.2)
	assertFloatAlmostEqual(t, float64(ts.Points()[0].Value), 18.35, 5.95)
	assertFloatAlmostEqual(t, float64(ts.Points()[1].Value), 30.35, 5.95)
	// 4 is a crazy one because it fluctuates between 60 and ~4k
	assertFloatAlmostEqual(t, float64(ts.Points()[5].Value), 4356.05, 500.0)
	ts = testCreateUpdateFetch(t, Last, 0.5, 300, 300, 200, 1, 0.5, 0.2)
	assertFloatAlmostEqual(t, float64(ts.Points()[0].Value), 0.7, 0.001)
	assertFloatAlmostEqual(t, float64(ts.Points()[10].Value), 2.7, 0.001)
	assertFloatAlmostEqual(t, float64(ts.Points()[20].Value), 4.7, 0.001)

}

/*
  Test the full cycle of creating a whisper file, adding some
  data points to it and then fetching a time series.
*/
func testCreateUpdateFetch(t *testing.T, aggregationMethod AggregationMethod, xFilesFactor float32, secondsAgo, fromAgo, fetchLength, step Duration, currentValue, increment Value) *TimeSeries {
	var whisper *Whisper
	var err error
	path, archiveList := setUpCreate(t)
	whisper, err = Create(path, archiveList, aggregationMethod, xFilesFactor, WithOpenFileFlag(os.O_RDWR))
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

	return ts
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

func TestFetchEmptyTimeseries(t *testing.T) {
	path, archiveList := setUpCreate(t)
	whisper, err := Create(path, archiveList, Sum, 0.5, WithOpenFileFlag(os.O_RDWR))
	if err != nil {
		t.Fatalf("Failed create: %v", err)
	}
	defer whisper.Close()

	if err := whisper.Sync(); err != nil {
		t.Fatal(err)
	}
	now := TimestampFromStdTime(time.Now())
	result, err := whisper.Fetch(now.Add(-3*Second), now)
	if err != nil {
		t.Fatal(err)
	}
	for _, point := range result.Points() {
		if !point.Value.IsNaN() {
			t.Fatalf("Expecting NaN values got '%v'", point.Value)
		}
	}
}

// Test for a bug in python whisper library: https://github.com/graphite-project/whisper/pull/136
func TestCreateUpdateFetchOneValue(t *testing.T) {
	var timeSeries *TimeSeries
	timeSeries = testCreateUpdateFetch(t, Average, 0.5, 3500, 3500, 1, 300, 0.5, 0.2)
	if len(timeSeries.Points()) > 1 {
		t.Fatalf("More then one point fetched\n")
	}
}

func TestCreateUpdateManyFetch(t *testing.T) {
	var timeSeries *TimeSeries

	points := makeGoodPoints(1000, 2, func(i int) float64 { return float64(i) })
	points = append(points, points[len(points)-1])
	timeSeries = testCreateUpdateManyFetch(t, Sum, 0.5, points, 1000, 800)

	// fmt.Println(timeSeries)

	assertFloatAlmostEqual(t, float64(timeSeries.Points()[0].Value), 455, 15)

	// all the ones
	points = makeGoodPoints(10000, 1, func(_ int) float64 { return 1 })
	timeSeries = testCreateUpdateManyFetch(t, Sum, 0.5, points, 10000, 10000)
	for i := 0; i < 6; i++ {
		assertFloatEqual(t, float64(timeSeries.Points()[i].Value), 1)
	}
	for i := 6; i < 10; i++ {
		assertFloatEqual(t, float64(timeSeries.Points()[i].Value), 5)
	}
}

func testCreateUpdateManyFetch(t *testing.T, aggregationMethod AggregationMethod, xFilesFactor float32, points Points, fromAgo, fetchLength Duration) *TimeSeries {
	var whisper *Whisper
	var err error
	path, archiveList := setUpCreate(t)
	whisper, err = Create(path, archiveList, aggregationMethod, xFilesFactor, WithOpenFileFlag(os.O_RDWR))
	if err != nil {
		t.Fatalf("Failed create: %v", err)
	}
	defer whisper.Close()

	now := TimestampFromStdTime(time.Now())

	if err := whisper.UpdateMany(points); err != nil {
		t.Fatal(err)
	}

	fromTime := now.Add(-fromAgo)
	untilTime := fromTime.Add(fetchLength)

	timeSeries, err := whisper.Fetch(fromTime, untilTime)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	return timeSeries
}

func makeGoodPoints(count int, step Duration, value func(int) float64) Points {
	points := make([]Point, count)
	now := TimestampFromStdTime(time.Now())
	for i := 0; i < count; i++ {
		points[i] = Point{Time: now.Add(-Duration(i) * step), Value: Value(value(i))}
	}
	return points
}

func makeBadPoints(count int, minAge Duration) Points {
	points := make([]Point, count)
	now := TimestampFromStdTime(time.Now())
	for i := 0; i < count; i++ {
		points[i] = Point{Time: now.Add(-(minAge + Duration(i))), Value: 123.456}
	}
	return points
}

func TestCreateUpdateManyOnly_old_points(t *testing.T) {
	points := makeBadPoints(1, 10000)

	path, archiveList := setUpCreate(t)
	whisper, err := Create(path, archiveList, Sum, 0.5, WithOpenFileFlag(os.O_RDWR))
	if err != nil {
		t.Fatalf("Failed create: %v", err)
	}
	defer whisper.Close()

	if err := whisper.UpdateMany(points); err != nil {
		t.Fatal(err)
	}
}

func Test_extractPoints(t *testing.T) {
	points := makeGoodPoints(100, 1, func(i int) float64 { return float64(i) })
	sort.Stable(points)
	now := TimestampFromStdTime(time.Now())
	currentPoints, remainingPoints := extractPoints(points, now, 50)
	if length := len(currentPoints); length != 50 {
		t.Fatalf("First: %v", length)
	}
	if length := len(remainingPoints); length != 50 {
		t.Fatalf("Second: %v", length)
	}
}

func Test_extractPoints_only_old_points(t *testing.T) {
	now := TimestampFromStdTime(time.Now())
	points := makeBadPoints(1, 100)
	sort.Stable(points)

	currentPoints, remainingPoints := extractPoints(points, now, 50)
	if length := len(currentPoints); length != 0 {
		t.Fatalf("First: %v", length)
	}
	if length := len(remainingPoints); length != 1 {
		t.Fatalf("Second2: %v", length)
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

func TestUpdateManyWithManyRetentions(t *testing.T) {
	path, archiveList := setUpCreate(t)
	lastArchive := archiveList[len(archiveList)-1]
	// log.Printf("lastArchive=%s, archiveList=%s", lastArchive, archiveList)

	valueMin := 41
	valueMax := 43

	whisper, err := Create(path, archiveList, Average, 0.5, WithOpenFileFlag(os.O_RDWR))
	if err != nil {
		t.Fatalf("Failed create: %v", err)
	}

	points := make([]Point, 1)
	now := TimestampFromStdTime(time.Now())
	for i := 0; i < int(lastArchive.secondsPerPoint*2); i++ {
		points[0] = Point{
			Time:  now.Add(-Duration(i) * Second),
			Value: Value(valueMin*(i%2) + valueMax*((i+1)%2)), // valueMin, valueMax, valueMin...
		}
		if err := whisper.UpdateMany(points); err != nil {
			t.Fatal(err)
		}
		// log.Printf("updated points=%v", points)
	}
	if err := whisper.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := whisper.Close(); err != nil {
		t.Fatal(err)
	}

	// check data in last archive
	whisper, err = Open(path)
	if err != nil {
		t.Fatalf("Failed open: %v", err)
	}
	defer whisper.Close()

	// log.Printf("before Fetch, from=%s, until=%s", now.Add(-lastArchive.MaxRetention()), now)
	result, err := whisper.Fetch(now.Add(-lastArchive.MaxRetention()), now)
	if err != nil {
		t.Fatalf("Failed fetch: %v", err)
	}

	foundValues := 0
	// log.Printf("len(result.Points())=%d", len(result.Points()))
	for i := 0; i < len(result.Points()); i++ {
		// log.Printf("i=%d, time=%s, value=%s", i, result.Points()[i].Time, result.Points()[i].Value)
		if !result.Points()[i].Value.IsNaN() {
			if result.Points()[i].Value >= Value(valueMin) &&
				result.Points()[i].Value <= Value(valueMax) {
				foundValues++
			}
		}
	}
	if foundValues < 2 {
		t.Fatalf("Not found values in archive %#v", lastArchive)
	}
}

func TestUpdateManyWithEqualTimestamp(t *testing.T) {
	now := TimestampFromStdTime(time.Now())
	points := Points{}

	// add points
	// now timestamp: 0,99,2,97,...,3,99,1
	// now-1 timestamp: 100,1,98,...,97,2,99

	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			points = append(points, Point{Time: now, Value: Value(i)})
			points = append(points, Point{Time: now.Add(-Second), Value: Value(100 - i)})
		} else {
			points = append(points, Point{Time: now, Value: Value(100 - i)})
			points = append(points, Point{Time: now.Add(-Second), Value: Value(i)})
		}
	}

	result := testCreateUpdateManyFetch(t, Average, 0.5, points, 2, 10)

	if result.Points()[0].Value != 99.0 {
		t.Errorf("Incorrect saved value. Expected %v, received %v", 99.0, result.Points()[0].Value)
	}
	if result.Points()[1].Value != 1.0 {
		t.Errorf("Incorrect saved value. Expected %v, received %v", 1.0, result.Points()[1].Value)
	}
}

func TestOpenValidatation(t *testing.T) {

	testOpen := func(data []byte) {
		path, _ := setUpCreate(t)
		defer func() {
			os.Remove(path)
		}()

		err := ioutil.WriteFile(path, data, 0777)
		if err != nil {
			t.Fatal(err)
		}

		wsp, err := Open(path)
		if wsp != nil {
			t.Fatal("Opened bad file")
		}
		if err == nil {
			t.Fatal("No error with file")
		}
	}

	// testWrite := func(data []byte) {
	// 	path, _ := setUpCreate(t)
	// 	defer func() {
	// 		os.Remove(path)
	// 	}()

	// 	err := ioutil.WriteFile(path, data, 0777)
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}

	// 	wsp, err := Open(path)
	// 	if wsp == nil || err != nil {
	// 		t.Fatalf("Open error: wsp=%v, err=%v", wsp, err)
	// 	}

	// 	err = wsp.Update(TimestampFromStdTime(time.Now()), 42)
	// 	if err == nil {
	// 		t.Fatal("Update broken wsp without error")
	// 	}

	// 	points := makeGoodPoints(1000, 2, func(i int) float64 { return float64(i) })
	// 	err = wsp.UpdateMany(points)
	// 	if err == nil {
	// 		t.Fatal("Update broken wsp without error")
	// 	}
	// }

	// Bad file with archiveCount = 1296223489
	testOpen([]byte{
		0xb8, 0x81, 0xd1, 0x1,
		0xc, 0x0, 0x1, 0x2,
		0x2e, 0x0, 0x0, 0x0,
		0x4d, 0x42, 0xcd, 0x1, // archiveCount
		0xc, 0x0, 0x2, 0x2,
	})

	fullHeader := []byte{
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
		0x00, 0x00, 0x00, 0x0c, // numberOfPoints
	}

	for i := 0; i < len(fullHeader); i++ {
		testOpen(fullHeader[:i])
	}

	// NOTE: We cannot use testWrite since our Open needs data with full body.
	// testWrite(fullHeader)
}

func TestUpdatePointForArchive(t *testing.T) {
	now := testParseTimestamp(t, "2020-07-03T06:00:38Z")
	wants := []string{
		`now:2020-07-03T06:00:38Z
retID:0	from:2020-07-03T06:00:31Z	until:2020-07-03T06:00:39Z	step:1s	values:NaN NaN NaN NaN NaN NaN NaN 1
retID:1	from:2020-07-03T06:00:08Z	until:2020-07-03T06:00:40Z	step:4s	values:NaN NaN NaN NaN NaN NaN NaN 1
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 1`,
		`now:2020-07-03T06:00:39Z
retID:0	from:2020-07-03T06:00:32Z	until:2020-07-03T06:00:40Z	step:1s	values:NaN NaN NaN NaN NaN NaN 1 2
retID:1	from:2020-07-03T06:00:08Z	until:2020-07-03T06:00:40Z	step:4s	values:NaN NaN NaN NaN NaN NaN NaN 3
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 3`,
		`now:2020-07-03T06:00:40Z
retID:0	from:2020-07-03T06:00:33Z	until:2020-07-03T06:00:41Z	step:1s	values:NaN NaN NaN NaN NaN 1 2 4
retID:1	from:2020-07-03T06:00:12Z	until:2020-07-03T06:00:44Z	step:4s	values:NaN NaN NaN NaN NaN NaN 3 4
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 7`,
		`now:2020-07-03T06:00:41Z
retID:0	from:2020-07-03T06:00:34Z	until:2020-07-03T06:00:42Z	step:1s	values:NaN NaN NaN NaN 1 2 4 8
retID:1	from:2020-07-03T06:00:12Z	until:2020-07-03T06:00:44Z	step:4s	values:NaN NaN NaN NaN NaN NaN 3 12
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 15`,
		`now:2020-07-03T06:00:42Z
retID:0	from:2020-07-03T06:00:35Z	until:2020-07-03T06:00:43Z	step:1s	values:NaN NaN NaN 1 2 4 8 16
retID:1	from:2020-07-03T06:00:12Z	until:2020-07-03T06:00:44Z	step:4s	values:NaN NaN NaN NaN NaN NaN 3 28
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 31`,
		`now:2020-07-03T06:00:43Z
retID:0	from:2020-07-03T06:00:36Z	until:2020-07-03T06:00:44Z	step:1s	values:NaN NaN 1 2 4 8 16 32
retID:1	from:2020-07-03T06:00:12Z	until:2020-07-03T06:00:44Z	step:4s	values:NaN NaN NaN NaN NaN NaN 3 60
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 63`,
		`now:2020-07-03T06:00:44Z
retID:0	from:2020-07-03T06:00:37Z	until:2020-07-03T06:00:45Z	step:1s	values:NaN 1 2 4 8 16 32 64
retID:1	from:2020-07-03T06:00:16Z	until:2020-07-03T06:00:48Z	step:4s	values:NaN NaN NaN NaN NaN 3 60 64
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 127`,
		`now:2020-07-03T06:00:45Z
retID:0	from:2020-07-03T06:00:38Z	until:2020-07-03T06:00:46Z	step:1s	values:1 2 4 8 16 32 64 128
retID:1	from:2020-07-03T06:00:16Z	until:2020-07-03T06:00:48Z	step:4s	values:NaN NaN NaN NaN NaN 3 60 192
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 255`,
		`now:2020-07-03T06:00:46Z
retID:0	from:2020-07-03T06:00:39Z	until:2020-07-03T06:00:47Z	step:1s	values:2 4 8 16 32 64 128 256
retID:1	from:2020-07-03T06:00:16Z	until:2020-07-03T06:00:48Z	step:4s	values:NaN NaN NaN NaN NaN 3 60 448
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 511`,
		`now:2020-07-03T06:00:47Z
retID:0	from:2020-07-03T06:00:40Z	until:2020-07-03T06:00:48Z	step:1s	values:4 8 16 32 64 128 256 512
retID:1	from:2020-07-03T06:00:16Z	until:2020-07-03T06:00:48Z	step:4s	values:NaN NaN NaN NaN NaN 3 60 960
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 1023`,
		`now:2020-07-03T06:00:48Z
retID:0	from:2020-07-03T06:00:41Z	until:2020-07-03T06:00:49Z	step:1s	values:8 16 32 64 128 256 512 1024
retID:1	from:2020-07-03T06:00:20Z	until:2020-07-03T06:00:52Z	step:4s	values:NaN NaN NaN NaN 3 60 960 1024
retID:2	from:2020-07-03T06:00:00Z	until:2020-07-03T06:01:04Z	step:16s	values:NaN NaN 1023 1024`,
		`now:2020-07-03T06:00:49Z
retID:0	from:2020-07-03T06:00:42Z	until:2020-07-03T06:00:50Z	step:1s	values:16 32 64 128 256 512 1024 2048
retID:1	from:2020-07-03T06:00:20Z	until:2020-07-03T06:00:52Z	step:4s	values:NaN NaN NaN NaN 3 60 960 3072
retID:2	from:2020-07-03T06:00:00Z	until:2020-07-03T06:01:04Z	step:16s	values:NaN NaN 1023 3072`,
		`now:2020-07-03T06:00:50Z
retID:0	from:2020-07-03T06:00:43Z	until:2020-07-03T06:00:51Z	step:1s	values:32 64 128 256 512 1024 2048 4096
retID:1	from:2020-07-03T06:00:20Z	until:2020-07-03T06:00:52Z	step:4s	values:NaN NaN NaN NaN 3 60 960 7168
retID:2	from:2020-07-03T06:00:00Z	until:2020-07-03T06:01:04Z	step:16s	values:NaN NaN 1023 7168`,
	}
	db := testCreateDB(t, "1s:8s,4s:32s,16s:64s", Sum, 0)
	archiveID := 0
	v := Value(1)
	for i, want := range wants {
		if err := db.UpdatePointForArchive(archiveID, now, v, now); err != nil {
			t.Fatal(err)
		}

		tsList := testFetchAllPoints(t, db, now)
		got := fmt.Sprintf("now:%s\n%s", now, timeSeriesListString(tsList))
		if got != want {
			t.Errorf("time series unmatch, i=%d,\n gotQuoted=%q,\nwantQuoted=%q,\n got=%s,\nwant=%s", i, got, want, got, want)
		}

		now = now.Add(Second)
		v *= 2
	}
}

func TestUpdatePointOld(t *testing.T) {
	now := testParseTimestamp(t, "2020-07-03T06:00:38Z")
	testCases := []struct {
		offset Duration
		want   string
	}{
		{
			offset: 0,
			want: `now:2020-07-03T06:00:38Z
retID:0	from:2020-07-03T06:00:31Z	until:2020-07-03T06:00:39Z	step:1s	values:NaN NaN NaN NaN NaN NaN NaN 1
retID:1	from:2020-07-03T06:00:08Z	until:2020-07-03T06:00:40Z	step:4s	values:NaN NaN NaN NaN NaN NaN NaN 1
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 1`,
		},
		{
			offset: 0,
			want: `now:2020-07-03T06:00:39Z
retID:0	from:2020-07-03T06:00:32Z	until:2020-07-03T06:00:40Z	step:1s	values:NaN NaN NaN NaN NaN NaN 1 2
retID:1	from:2020-07-03T06:00:08Z	until:2020-07-03T06:00:40Z	step:4s	values:NaN NaN NaN NaN NaN NaN NaN 3
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 3`,
		},
		{
			offset: 0,
			want: `now:2020-07-03T06:00:40Z
retID:0	from:2020-07-03T06:00:33Z	until:2020-07-03T06:00:41Z	step:1s	values:NaN NaN NaN NaN NaN 1 2 4
retID:1	from:2020-07-03T06:00:12Z	until:2020-07-03T06:00:44Z	step:4s	values:NaN NaN NaN NaN NaN NaN 3 4
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 7`,
		},
		{
			offset: 0,
			want: `now:2020-07-03T06:00:41Z
retID:0	from:2020-07-03T06:00:34Z	until:2020-07-03T06:00:42Z	step:1s	values:NaN NaN NaN NaN 1 2 4 8
retID:1	from:2020-07-03T06:00:12Z	until:2020-07-03T06:00:44Z	step:4s	values:NaN NaN NaN NaN NaN NaN 3 12
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 15`,
		},
		{
			offset: 0,
			want: `now:2020-07-03T06:00:42Z
retID:0	from:2020-07-03T06:00:35Z	until:2020-07-03T06:00:43Z	step:1s	values:NaN NaN NaN 1 2 4 8 16
retID:1	from:2020-07-03T06:00:12Z	until:2020-07-03T06:00:44Z	step:4s	values:NaN NaN NaN NaN NaN NaN 3 28
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 31`,
		},
		{
			offset: 0,
			want: `now:2020-07-03T06:00:43Z
retID:0	from:2020-07-03T06:00:36Z	until:2020-07-03T06:00:44Z	step:1s	values:NaN NaN 1 2 4 8 16 32
retID:1	from:2020-07-03T06:00:12Z	until:2020-07-03T06:00:44Z	step:4s	values:NaN NaN NaN NaN NaN NaN 3 60
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 63`,
		},
		{
			offset: 0,
			want: `now:2020-07-03T06:00:44Z
retID:0	from:2020-07-03T06:00:37Z	until:2020-07-03T06:00:45Z	step:1s	values:NaN 1 2 4 8 16 32 64
retID:1	from:2020-07-03T06:00:16Z	until:2020-07-03T06:00:48Z	step:4s	values:NaN NaN NaN NaN NaN 3 60 64
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 127`,
		},
		{
			offset: 0,
			want: `now:2020-07-03T06:00:45Z
retID:0	from:2020-07-03T06:00:38Z	until:2020-07-03T06:00:46Z	step:1s	values:1 2 4 8 16 32 64 128
retID:1	from:2020-07-03T06:00:16Z	until:2020-07-03T06:00:48Z	step:4s	values:NaN NaN NaN NaN NaN 3 60 192
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 255`,
		},
		{
			offset: -7 * Second,
			want: `now:2020-07-03T06:00:46Z
retID:0	from:2020-07-03T06:00:39Z	until:2020-07-03T06:00:47Z	step:1s	values:256 4 8 16 32 64 128 NaN
retID:1	from:2020-07-03T06:00:16Z	until:2020-07-03T06:00:48Z	step:4s	values:NaN NaN NaN NaN NaN 256 60 448
retID:2	from:2020-07-03T05:59:44Z	until:2020-07-03T06:00:48Z	step:16s	values:NaN NaN NaN 511`,
		},
	}
	db := testCreateDB(t, "1s:8s,4s:32s,16s:64s", Sum, 0)
	archiveID := 0
	v := Value(1)
	for i, tc := range testCases {
		if err := db.UpdatePointForArchive(archiveID, now.Add(tc.offset), v, now); err != nil {
			t.Fatal(err)
		}

		tsList := testFetchAllPoints(t, db, now)
		got := fmt.Sprintf("now:%s\n%s", now, timeSeriesListString(tsList))
		if got != tc.want {
			t.Errorf("time series unmatch, i=%d,\n gotQuoted=%q,\nwantQuoted=%q,\n got=%s,\nwant=%s", i, got, tc.want, got, tc.want)
		}

		now = now.Add(Second)
		v *= 2
	}
}

func TestUpdatePointOld2(t *testing.T) {
	db := testCreateDB(t, "1s:2s,2s:4s", Sum, 0)
	now := time.Now()
	targetTime := now.Truncate(2 * time.Second).Add(2 * time.Second)
	time.Sleep(targetTime.Sub(now))

	var err error
	err = db.Update(TimestampFromStdTime(time.Now()), 1)
	if err != nil {
		t.Fatal(err)
	}
	testViewWhisper(t, db)

	time.Sleep(time.Second)
	err = db.Update(TimestampFromStdTime(time.Now()), 2)
	if err != nil {
		t.Fatal(err)
	}
	testViewWhisper(t, db)

	time.Sleep(time.Second)
	err = db.Update(TimestampFromStdTime(time.Now()), 4)
	if err != nil {
		t.Fatal(err)
	}
	testViewWhisper(t, db)

	err = db.Update(TimestampFromStdTime(time.Now()), 16)
	if err != nil {
		t.Fatal(err)
	}
	testViewWhisper(t, db)

	err = db.Update(TimestampFromStdTime(time.Now()).Add(-Second), 32)
	if err != nil {
		t.Fatal(err)
	}
	testViewWhisper(t, db)
}

func testViewWhisper(t *testing.T, db *Whisper) {
	t.Helper()

	untilTime := TimestampFromStdTime(time.Now())
	fromTime := untilTime.Add(-2 * Second)
	ts, err := db.Fetch(fromTime, untilTime)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("-----------------\n archive0 ts=%v", ts)

	fromTime = untilTime.Add(-4 * Second)
	ts, err = db.Fetch(fromTime, untilTime)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("-----------------\n archive1 ts=%v", ts)
}

func testParseTimestamp(t *testing.T, s string) Timestamp {
	t.Helper()
	ts, err := ParseTimestamp(s)
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

func timeSeriesListString(tsList []*TimeSeries) string {
	var b strings.Builder
	for retID, ts := range tsList {
		if retID > 0 {
			b.WriteRune('\n')
		}
		fmt.Fprintf(&b, "retID:%d\tfrom:%s\tuntil:%s\tstep:%s\tvalues:",
			retID, ts.FromTime(), ts.UntilTime(), ts.Step())
		for i, pt := range ts.Points() {
			if i > 0 {
				b.WriteRune(' ')
			}
			fmt.Fprintf(&b, "%v", pt.Value)
		}
	}
	return b.String()
}

func testCreateDB(t *testing.T, archiveInfoListDefs string, aggMethod AggregationMethod, xFilesFactor float32) *Whisper {
	t.Helper()

	archiveInfoList, err := ParseArchiveInfoList(archiveInfoListDefs)
	if err != nil {
		t.Fatal(err)
	}

	file, err := ioutil.TempFile("", "whispertool-test-*.wsp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.Remove(file.Name())
	})
	db, err := Create(file.Name(), archiveInfoList, aggMethod, xFilesFactor, WithOpenFileFlag(os.O_RDWR))
	if err != nil {
		t.Fatal(err)
	}

	return db
}

func testFetchAllPoints(t *testing.T, db *Whisper, now Timestamp) []*TimeSeries {
	tsList := make([]*TimeSeries, len(db.ArchiveInfoList()))
	for archiveID, a := range db.ArchiveInfoList() {
		from := now.Add(-a.MaxRetention())
		until := now
		ts, err := db.FetchFromArchive(archiveID, from, until, now)
		if err != nil {
			t.Fatal(err)
		}
		tsList[archiveID] = ts
	}

	for archiveID, a := range db.ArchiveInfoList() {
		ts := tsList[archiveID]
		if got, want := len(ts.Points()), int(a.NumberOfPoints()); got != want {
			t.Errorf("points length unmatched, now=%s, retID=%d, got=%d, want=%d", now, archiveID, got, want)
		}
		if got, want := ts.FromTime(), a.interval(now.Add(-a.MaxRetention())); got != want {
			t.Errorf("fromTime unmatched, now=%s, retID=%d, got=%s, want=%s", now, archiveID, got, want)
		}
		if got, want := ts.UntilTime(), a.interval(now); got != want {
			t.Errorf("until unmatched, now=%s, retID=%d, got=%s, want=%s", now, archiveID, got, want)
		}
		if got, want := ts.Step(), a.SecondsPerPoint(); got != want {
			t.Errorf("step unmatched, now=%s, retID=%d, got=%s, want=%s", now, archiveID, got, want)
		}
	}
	return tsList
}
