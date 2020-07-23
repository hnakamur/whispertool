package compattest

import (
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hnakamur/whispertool"
	"github.com/hnakamur/whispertool/cmd"
)

func TestGoWhisperUpdateMany(t *testing.T) {
	dir, err := ioutil.TempDir("", "whispertool-compat-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})

	clock.Set(time.Date(2020, 7, 23, 6, 5, 1, 0, time.UTC))

	db, err := CreateGoWhisperDB(filepath.Join(dir, whispertoolFilename), "1s:2s,2s:4s,4s:8s", "sum", 0.5)
	if err != nil {
		t.Fatal(err)
	}

	archiveID := 1
	points := whispertool.Points{
		{
			Time:  whispertool.TimestampFromStdTime(time.Date(2020, 7, 23, 6, 4, 57, 0, time.UTC)),
			Value: whispertool.Value(1),
		},
	}
	err = db.UpdatePointsForArchive(points, archiveID)
	if err != nil {
		t.Fatal(err)
	}

	gotTL, err := db.fetchAllArchives()
	if err != nil {
		t.Fatal(err)
	}

	wantTL := cmd.TimeSeriesList{
		whispertool.NewTimeSeries(
			whispertool.TimestampFromStdTime(time.Date(2020, 7, 23, 6, 5, 0, 0, time.UTC)),
			whispertool.TimestampFromStdTime(time.Date(2020, 7, 23, 6, 5, 2, 0, time.UTC)),
			whispertool.Second,
			[]whispertool.Value{whispertool.Value(math.NaN()), whispertool.Value(math.NaN())},
		),
		whispertool.NewTimeSeries(
			whispertool.TimestampFromStdTime(time.Date(2020, 7, 23, 6, 4, 58, 0, time.UTC)),
			whispertool.TimestampFromStdTime(time.Date(2020, 7, 23, 6, 5, 2, 0, time.UTC)),
			whispertool.Duration(2)*whispertool.Second,
			[]whispertool.Value{whispertool.Value(math.NaN()), whispertool.Value(math.NaN())},
		),
		whispertool.NewTimeSeries(
			whispertool.TimestampFromStdTime(time.Date(2020, 7, 23, 6, 4, 56, 0, time.UTC)),
			whispertool.TimestampFromStdTime(time.Date(2020, 7, 23, 6, 5, 4, 0, time.UTC)),
			whispertool.Duration(4)*whispertool.Second,
			[]whispertool.Value{whispertool.Value(1), whispertool.Value(math.NaN())},
		),
	}
	if !gotTL.Equal(wantTL) {
		pl1, pl2 := gotTL.Diff(wantTL)
		t.Fatalf("timeSeries unmatch,\npl1=%s,\npl2=%s\n gotTL=%s\nwantTL=%s", pl1, pl2, gotTL, wantTL)
	}
}
