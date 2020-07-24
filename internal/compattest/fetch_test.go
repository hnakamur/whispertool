package compattest

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/hnakamur/whispertool"
	"pgregory.net/rapid"
)

func TestCompatFetch(t *testing.T) {
	rapid.Check(t, rapid.Run(&fetchMachine{}))
}

type fetchMachine struct {
	dir string
	db1 *WhispertoolDB
	db2 *GoWhisperDB
}

func (m *fetchMachine) Init(t *rapid.T) {
	dir, err := ioutil.TempDir("", "whispertool-compat-test")
	if err != nil {
		t.Fatal(err)
	}
	m.dir = dir

	m.db1, m.db2 = bothCreate(t, dir, "1s:1m,1m:1h,1h:1d", "sum", 0)
	clock.Set(time.Now())
	t.Logf("Init set now=%s", whispertool.TimestampFromStdTime(time.Now()))

	for archiveID := range m.db1.ArciveInfoList() {
		points := NewAllFilledPointsForArchiveGenerator(m.db1, archiveID).Draw(t, "points").(Points)
		bothUpdatePointsForArchive(t, m.db1, m.db2, points, archiveID)
	}
}

func (m *fetchMachine) Cleanup() {
	os.RemoveAll(m.dir)
}

func (m *fetchMachine) MayAdvanceTime(t *rapid.T) {
	timeAdvanceRatio := rapid.Float64Range(0, 1).Draw(t, "timeAdvanceRatio").(float64)
	if timeAdvanceRatio < 0.05 {
		clock.Sleep(time.Second)
		t.Logf("MayAdvanceTime now=%s", whispertool.TimestampFromStdTime(time.Now()))
	}
}

func (m *fetchMachine) Check(t *rapid.T) {
	r := NewTimestampRangeGenerator(m.db1).Draw(t, "timestampRange").(TimestampRange)
	ts1, ts2 := bothFetch(t, m.db1, m.db2, r.From, r.Until)
	if ts1 == nil && ts2 == nil {
		// This is OK
		return
	}
	if ts1 == nil {
		t.Fatalf("ts1 is nil")
	}
	if ts2 == nil {
		t.Fatalf("ts2 is nil")
	}
	if !ts1.EqualTimeRangeAndStep(ts2) {
		t.Fatalf("timeSeries time range or step unmatch,\n got=%s,\nwant=%s",
			TimeRangeStepString(ts1), TimeRangeStepString(ts2))
	}
	if !ts1.Equal(ts2) {
		pl1, pl2 := ts1.DiffPoints(ts2)
		t.Fatalf("timeSeries unmatch,\npl1=%s,\npl2=%s", pl1, pl2)
	}
}

func TimeRangeStepString(ts *whispertool.TimeSeries) string {
	return fmt.Sprintf("{fromTime:%s untilTime:%s step:%s}",
		ts.FromTime(), ts.UntilTime(), ts.Step())
}
