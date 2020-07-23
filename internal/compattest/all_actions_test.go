package compattest

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/hnakamur/whispertool"
	"pgregory.net/rapid"
)

func TestCompatAllActions(t *testing.T) {
	t.Run("small_sum_0", func(t *testing.T) {
		rapid.Check(t, rapid.Run(&allActionsMachineSmallSum0{}))
	})
	t.Run("small_sum_0.5", func(t *testing.T) {
		rapid.Check(t, rapid.Run(&allActionsMachineSmallSum05{}))
	})
	t.Run("small_sum_1", func(t *testing.T) {
		rapid.Check(t, rapid.Run(&allActionsMachineSmallSum1{}))
	})
	t.Run("big_sum_0", func(t *testing.T) {
		rapid.Check(t, rapid.Run(&allActionsMachineBigSum0{}))
	})
	t.Run("big_sum_0.5", func(t *testing.T) {
		rapid.Check(t, rapid.Run(&allActionsMachineBigSum05{}))
	})
	t.Run("big_sum_1", func(t *testing.T) {
		rapid.Check(t, rapid.Run(&allActionsMachineBigSum1{}))
	})
}

type allActionsMachineSmallSum0 struct{ allActionsMachine }

func (m *allActionsMachineSmallSum0) Init(t *rapid.T) {
	m.allActionsMachine.retentionDefs = "1s:2s,2s:4s,4s:8s"
	m.allActionsMachine.aggregationMethod = "sum"
	m.allActionsMachine.xFilesFactor = 0.0
	m.allActionsMachine.Init(t)
}

type allActionsMachineSmallSum05 struct{ allActionsMachine }

func (m *allActionsMachineSmallSum05) Init(t *rapid.T) {
	m.allActionsMachine.retentionDefs = "1s:2s,2s:4s,4s:8s"
	m.allActionsMachine.aggregationMethod = "sum"
	m.allActionsMachine.xFilesFactor = 0.5
	m.allActionsMachine.Init(t)
}

type allActionsMachineSmallSum1 struct{ allActionsMachine }

func (m *allActionsMachineSmallSum1) Init(t *rapid.T) {
	m.allActionsMachine.retentionDefs = "1s:2s,2s:4s,4s:8s"
	m.allActionsMachine.aggregationMethod = "sum"
	m.allActionsMachine.xFilesFactor = 1
	m.allActionsMachine.Init(t)
}

type allActionsMachineBigSum0 struct{ allActionsMachine }

func (m *allActionsMachineBigSum0) Init(t *rapid.T) {
	m.allActionsMachine.retentionDefs = "1s:1m,1m:1h,1h:1d"
	m.allActionsMachine.aggregationMethod = "sum"
	m.allActionsMachine.xFilesFactor = 0.0
	m.allActionsMachine.Init(t)
}

type allActionsMachineBigSum05 struct{ allActionsMachine }

func (m *allActionsMachineBigSum05) Init(t *rapid.T) {
	m.allActionsMachine.retentionDefs = "1s:1m,1m:1h,1h:1d"
	m.allActionsMachine.aggregationMethod = "sum"
	m.allActionsMachine.xFilesFactor = 0.5
	m.allActionsMachine.Init(t)
}

type allActionsMachineBigSum1 struct{ allActionsMachine }

func (m *allActionsMachineBigSum1) Init(t *rapid.T) {
	m.allActionsMachine.retentionDefs = "1s:1m,1m:1h,1h:1d"
	m.allActionsMachine.aggregationMethod = "sum"
	m.allActionsMachine.xFilesFactor = 1.0
	m.allActionsMachine.Init(t)
}

type allActionsMachine struct {
	retentionDefs     string
	aggregationMethod string
	xFilesFactor      float32

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

	t.Logf("Init retentionDefs=%s, aggregationMethod=%s, xFilesFactor=%s", m.retentionDefs, m.aggregationMethod, strconv.FormatFloat(float64(m.xFilesFactor), 'f', -1, 32))
	m.db1, m.db2 = bothCreate(t, dir, m.retentionDefs, m.aggregationMethod, m.xFilesFactor)
	clock.Set(time.Now())
	t.Logf("Init set now=%s", whispertool.TimestampFromStdTime(time.Now()))
}

func (m *allActionsMachine) Cleanup() {
	os.RemoveAll(m.dir)
}

func (m *allActionsMachine) Update(t *rapid.T) {
	pt := NewPointGenerator(m.db1).Example(0).(whispertool.Point)
	bothUpdate(t, m.db1, m.db2, pt.Time, pt.Value)
}

func (m *allActionsMachine) UpdateMany(t *rapid.T) {
	// points := NewPointsForAllArchivesGenerator(m.db1).Draw(t, "points").(Points)
	archiveID := rapid.IntRange(0, len(m.db1.ArciveInfoList())-1).Draw(t, "archiveID").(int)
	points := NewPointsForArchiveGenerator(m.db1, archiveID).Example(0).(Points)
	bothUpdatePointsForArchive(t, m.db1, m.db2, points, archiveID)
}

func (m *allActionsMachine) SleepSecond(t *rapid.T) {
	clock.Sleep(time.Second)
	t.Logf("SleepSecond now=%s", whispertool.TimestampFromStdTime(clock.Now()))
}

func (m *allActionsMachine) Check(t *rapid.T) {
	tl1, tl2 := bothFetchAllArchives(t, m.db1, m.db2)
	if !tl1.AllEqualTimeRangeAndStep(tl2) {
		t.Fatalf("timeSeries time range or step unmatch,\n got=%s,\nwant=%s",
			tl1.TimeRangeStepString(), tl2.TimeRangeStepString())
	}
	if !tl1.Equal(tl2) {
		pl1, pl2 := tl1.Diff(tl2)
		t.Fatalf("timeSeries unmatch,\npl1=%s,\npl2=%s\ntl1=%s\ntl2=%s", pl1, pl2, tl1, tl2)
	}
}
