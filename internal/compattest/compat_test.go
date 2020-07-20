package compattest

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/go-graphite/go-whisper"
	"github.com/hnakamur/whispertool"
)

var clock = &fixedClock{}

func TestCompatUpdate(t *testing.T) {
	dir := tempDir(t)
	db1, db2 := bothCreate(t, dir, "1s:2s,2s:4s", "sum", 0)

	clock.Set(time.Date(2020, 7, 20, 23, 48, 01, 0, time.Local))
	now := whispertool.TimestampFromStdTime(clock.Now())
	bothUpdate(t, db1, db2, now, whispertool.Value(1))

	clock.Sleep(time.Second)
	now = whispertool.TimestampFromStdTime(clock.Now())
	bothUpdate(t, db1, db2, now, whispertool.Value(2))

	ts1, ts2 := bothFetchAllArchives(t, db1, db2)
	if !ts1.Equal(ts2) {
		t.Logf("timeSeries unmatch,\n got=%s,\nwant=%s", ts1, ts2)
	}
}

func tempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "whispertool-compat-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}

func TestMain(m *testing.M) {
	whispertool.Now = clock.Now
	whisper.Now = clock.Now
	os.Exit(m.Run())
}
