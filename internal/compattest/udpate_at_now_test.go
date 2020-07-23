package compattest

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/hnakamur/whispertool"
	"pgregory.net/rapid"
)

func TestCompatUpdateAtNow(t *testing.T) {
	rapid.Check(t, rapid.Run(&updateAtNowMachine{}))
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

	m.db1, m.db2 = bothCreate(t, dir, "1s:1m,1m:1h,1h:1d", "sum", 0)
	clock.Set(time.Now())
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
	tl1, tl2 := bothFetchAllArchives(t, m.db1, m.db2)
	if !tl1.AllEqualTimeRangeAndStep(tl2) {
		t.Fatalf("timeSeries time range or step unmatch,\n got=%s,\nwant=%s",
			tl1.TimeRangeStepString(), tl2.TimeRangeStepString())
	}
	if !tl1.Equal(tl2) {
		pl1, pl2 := tl1.Diff(tl2)
		t.Fatalf("timeSeries unmatch,\npl1=%s,\npl2=%s", pl1, pl2)
	}
}
