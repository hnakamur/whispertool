package whispertool

import (
	crand "crypto/rand"
	"encoding/binary"
	"io"
	"io/ioutil"
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

// func TestOpenFile(t *testing.T) {
// 	path, retentions := setUpCreate(t)
// 	whisper1, err := Create(path, retentions, Average, 0.5)
// 	if err != nil {
// 		t.Errorf("Failed to create: %v", err)
// 	}

// 	// write some points
// 	now := int(time.Now().Unix())
// 	for i := 0; i < 2; i++ {
// 		whisper1.Update(100, now-(i*1))
// 	}

// 	whisper2, err := Open(path)
// 	if err != nil {
// 		t.Fatalf("Failed to open whisper file: %v", err)
// 	}
// 	if whisper1.aggregationMethod != whisper2.aggregationMethod {
// 		t.Fatalf("aggregationMethod did not match, expected %v, received %v", whisper1.aggregationMethod, whisper2.aggregationMethod)
// 	}
// 	if whisper1.maxRetention != whisper2.maxRetention {
// 		t.Fatalf("maxRetention did not match, expected %v, received %v", whisper1.maxRetention, whisper2.maxRetention)
// 	}
// 	if whisper1.xFilesFactor != whisper2.xFilesFactor {
// 		t.Fatalf("xFilesFactor did not match, expected %v, received %v", whisper1.xFilesFactor, whisper2.xFilesFactor)
// 	}
// 	if len(whisper1.archives) != len(whisper2.archives) {
// 		t.Fatalf("archive count does not match, expected %v, received %v", len(whisper1.archives), len(whisper2.archives))
// 	}
// 	for i := range whisper1.archives {
// 		if whisper1.archives[i].offset != whisper2.archives[i].offset {
// 			t.Fatalf("archive mismatch offset at %v [%v, %v]", i, whisper1.archives[i].offset, whisper2.archives[i].offset)
// 		}
// 		if whisper1.archives[i].Retention.secondsPerPoint != whisper2.archives[i].Retention.secondsPerPoint {
// 			t.Fatalf("Retention.secondsPerPoint mismatch offset at %v [%v, %v]", i, whisper1.archives[i].Retention.secondsPerPoint, whisper2.archives[i].Retention.secondsPerPoint)
// 		}
// 		if whisper1.archives[i].Retention.numberOfPoints != whisper2.archives[i].Retention.numberOfPoints {
// 			t.Fatalf("Retention.numberOfPoints mismatch offset at %v [%v, %v]", i, whisper1.archives[i].Retention.numberOfPoints, whisper2.archives[i].Retention.numberOfPoints)
// 		}

// 	}

// 	result1, err := whisper1.Fetch(now-3, now)
// 	if err != nil {
// 		t.Fatalf("Error retrieving result from created whisper")
// 	}
// 	result2, err := whisper2.Fetch(now-3, now)
// 	if err != nil {
// 		t.Fatalf("Error retrieving result from opened whisper")
// 	}

// 	if result1.String() != result2.String() {
// 		t.Fatalf("Results do not match")
// 	}

// 	tearDown()
// }
