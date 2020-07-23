package compattest

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/hnakamur/whispertool"
	"pgregory.net/rapid"
)

func TestCompatAllActions(t *testing.T) {
	rapid.Check(t, rapid.Run(&allActionsMachine{}))
}

type allActionsMachine struct {
	dir string
	db1 *WhispertoolDB
	db2 *GoWhisperDB
}

func (m *allActionsMachine) Init(t *rapid.T) {
	dir, err := ioutil.TempDir("", "whispertool-compat-test")
	if err != nil {
		t.Fatal(err)
	}
	m.dir = dir

	m.db1, m.db2 = bothCreate(t, dir, "1s:30h,1h:32d,1d:400d", "sum", 0.5)
	clock.Set(time.Now())
}

func (m *allActionsMachine) Cleanup() {
	os.RemoveAll(m.dir)
}

func (m *allActionsMachine) Update(t *rapid.T) {
	now := whispertool.TimestampFromStdTime(clock.Now())
	oldest := now.Add(-(m.db1.db.MaxRetention() - m.db1.ArciveInfoList()[0].SecondsPerPoint()))
	timestamp := whispertool.Timestamp(rapid.Uint32Range(uint32(oldest), uint32(now)).Draw(t, "timestamp").(uint32))
	v := rapid.Float64().Draw(t, "v").(float64)
	bothUpdate(t, m.db1, m.db2, timestamp, whispertool.Value(v))
}

func (m *allActionsMachine) SleepSecond(t *rapid.T) {
	clock.Sleep(time.Second)
}

func (m *allActionsMachine) Check(t *rapid.T) {
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
