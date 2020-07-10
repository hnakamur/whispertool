package cmd

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hnakamur/whispertool"
)

func TestCopy(t *testing.T) {
	tempdir, err := ioutil.TempDir("", "whispertool-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err != nil {
			t.Logf("We leave temp dir %s for you to investigate, err=%v", tempdir, err)
			return
		}
		if err := os.RemoveAll(tempdir); err != nil {
			t.Fatal(err)
		}
	})

	srcBase := filepath.Join(tempdir, "src")
	destBase := filepath.Join(tempdir, "dest")
	item := "item1"
	src := "sv01.wsp"
	dest := src
	retentionDefs := "1m:30h,1h:32d,1d:400d"

	srcFullPath := filepath.Join(srcBase, item, src)
	if err := os.MkdirAll(filepath.Dir(srcFullPath), 0700); err != nil {
		t.Fatal(err)
	}

	destFullPath := filepath.Join(destBase, item, dest)
	if err := os.MkdirAll(filepath.Dir(destFullPath), 0700); err != nil {
		t.Fatal(err)
	}

	now := whispertool.TimestampFromStdTime(time.Now())

	genSrcCmd := &GenerateCommand{
		Dest:          srcFullPath,
		Perm:          0644,
		RetentionDefs: retentionDefs,
		RandMax:       1000,
		Fill:          true,
		Now:           now,
		TextOut:       "",
	}
	if err = genSrcCmd.Execute(); err != nil {
		t.Fatal(err)
	}

	genDestCmd := &GenerateCommand{
		Dest:          destFullPath,
		Perm:          0644,
		RetentionDefs: retentionDefs,
		RandMax:       1000,
		Fill:          true,
		Now:           now,
		TextOut:       "",
	}
	if err = genDestCmd.Execute(); err != nil {
		t.Fatal(err)
	}

	copyCmd := &CopyCommand{
		SrcBase:     srcBase,
		SrcRelPath:  filepath.Join(item, src),
		DestBase:    destBase,
		DestRelPath: filepath.Join(item, dest),
		From:        0,
		Until:       now,
		Now:         now.Add(-5*whispertool.Minute),
		ArchiveID:   ArchiveIDAll,
		TextOut:     "",
	}
	if err = copyCmd.Execute(); err != nil {
		t.Fatal(err)
	}

	diffCmd := &DiffCommand{
		SrcBase:     srcBase,
		SrcRelPath:  filepath.Join(item, src),
		DestBase:    destBase,
		DestRelPath: filepath.Join(item, dest),
		From:        0,
		Until:       now,
		Now:         now,
		ArchiveID:   ArchiveIDAll,
		TextOut:     "-",
	}
	if err = diffCmd.Execute(); err != nil {
		t.Fatal(err)
	}
}
