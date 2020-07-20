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
		rapid.Check(t, rapid.Run(&updateAtNowMachine{}))
	})
}

type updateAtNowMachine struct {
	dir string
	db1 *WhispertoolDB
	db2 *GoWhisperDB
}

func (m *updateAtNowMachine) Init(t *rapid.T) {
	dir, err := ioutil.TempDir("", "whispertool-compat-test")
	if err != nil {
		t.Fatal(err)
	}
	m.dir = dir

	m.db1, m.db2 = bothCreate(t, dir, "1s:2s,2s:4s", "sum", 0)
	clock.Set(time.Now().Truncate(time.Second))
}

func (m *updateAtNowMachine) Cleanup() {
	os.RemoveAll(m.dir)
}

func (m *updateAtNowMachine) Update(t *rapid.T) {
	clock.Sleep(time.Second)
	now := whispertool.TimestampFromStdTime(clock.Now())
	v := rapid.Float64().Draw(t, "v").(float64)
	bothUpdate(t, m.db1, m.db2, now, whispertool.Value(v))
}

func (m *updateAtNowMachine) Check(t *rapid.T) {
	ts1, ts2 := bothFetchAllArchives(t, m.db1, m.db2)
	if !ts1.Equal(ts2) {
		t.Fatalf("timeSeries unmatch,\n got=%s,\nwant=%s", ts1, ts2)
	}
}

func TestMain(m *testing.M) {
	whispertool.Now = clock.Now
	whisper.Now = clock.Now
	os.Exit(m.Run())
}
