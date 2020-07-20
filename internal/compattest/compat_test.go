package compattest

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/go-graphite/go-whisper"
	"github.com/hnakamur/whispertool"
	"pgregory.net/rapid"
)

var clock = &fixedClock{}

func TestCompatUpdate(t *testing.T) {
	t.Run("updateAtNow", func(t *testing.T) {
		rapid.Check(t, func(rt *rapid.T) {
			dir := tempDir(t)
			db1, db2 := bothCreate(t, dir, "1s:2s,2s:4s", "sum", 0)

			clock.Set(time.Now().Truncate(time.Second))
			c := rapid.IntRange(4, 8).Draw(rt, "c").(int)
			for i := 0; i < c; i++ {
				v := rapid.Float64().Draw(rt, "v").(float64)
				now := whispertool.TimestampFromStdTime(clock.Now())
				bothUpdate(t, db1, db2, now, whispertool.Value(v))
				ts1, ts2 := bothFetchAllArchives(t, db1, db2)
				if !ts1.Equal(ts2) {
					t.Fatalf("timeSeries unmatch,\n got=%s,\nwant=%s", ts1, ts2)
				}

				clock.Sleep(time.Second)
			}
		})
	})
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
