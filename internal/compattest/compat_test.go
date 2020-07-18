package compattest

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/hnakamur/whispertool"
)

func TestCompatUpdate(t *testing.T) {
	dir := testTempDir(t)
	db1, db2, err := BothCreate(dir, "1s:2s,2s:4s", "sum", 0)
	if err != nil {
		t.Fatal(err)
	}

	now := whispertool.TimestampFromStdTime(time.Now())
	if err := BothUpdate(db1, db2, now, whispertool.Value(1)); err != nil {
		t.Fatal(err)
	}

	ts1, ts2, err := BothFetchAllArchives(db1, db2, now)
	if err != nil {
		t.Fatal(err)
	}

	if ts1.Equal(ts2) {
		t.Logf("match ts1 ts2")
	} else {
		t.Logf("unmatch ts1 ts2")
	}
	t.Logf("ts1=%s, ts2=%s", ts1, ts2)
}

func testTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "whispertool-compat-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}
