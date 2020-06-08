package whispertool_test

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hnakamur/whispertool"
)

func TestMerge(t *testing.T) {
	dir, err := ioutil.TempDir("", "whispertool_test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})

	srcFilename := filepath.Join(dir, "src.wsp")
	destFilename := filepath.Join(dir, "dest.wsp")

	const retentionDefs = "1m:2h,1h:2d,1d:30d"
	const fill = true
	const randMax = 100
	err = whispertool.Generate(srcFilename, retentionDefs, fill, randMax)
	if err != nil {
		t.Fatal(err)
	}

	const emptyRate = 0.2
	now := time.Now()
	until := now
	from := time.Unix(0, 0)
	err = whispertool.Hole(srcFilename, destFilename, emptyRate, now, from, until)
	if err != nil {
		t.Fatal(err)
	}

	const recursive = false
	err = whispertool.Merge(srcFilename, destFilename, recursive, now, from, until)
	if err != nil {
		t.Fatal(err)
	}

	const ignoreSrcEmpty = false
	const showAll = false
	err = whispertool.Diff(srcFilename, destFilename, recursive, ignoreSrcEmpty, showAll, now, from, until)
	if err != nil {
		if errors.Is(err, whispertool.ErrDiffFound) {
			t.Errorf("merged result dest.wsp should have same content as src.wsp: %s", err)
		} else {
			t.Fatal(err)
		}
	}
}
