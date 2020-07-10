package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hnakamur/whispertool"
)

func TestCopyCommand(t *testing.T) {
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
	retentionDefs := "1m:30h,1h:32d,1d:400d"

	if err := os.MkdirAll(filepath.Join(srcBase, item), 0700); err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(filepath.Join(destBase, item), 0700); err != nil {
		t.Fatal(err)
	}

	now := whispertool.TimestampFromStdTime(time.Now())

	testCases := []struct {
		untilOffset whispertool.Duration
	}{
		{untilOffset: 0},
		{untilOffset: 30*whispertool.Hour - whispertool.Second},
		{untilOffset: 30 * whispertool.Hour},
		{untilOffset: 30*whispertool.Hour + whispertool.Second},
		{untilOffset: 32*whispertool.Day - whispertool.Second},
		{untilOffset: 32 * whispertool.Day},
		{untilOffset: 32*whispertool.Day + whispertool.Second},
		{untilOffset: 400*whispertool.Day - whispertool.Second},
	}
	for _, tc := range testCases {
		untilOffset := tc.untilOffset
		t.Run("untilOffset"+untilOffset.String(), func(t *testing.T) {
			t.Parallel()
			src := fmt.Sprintf("uo%s.wsp", untilOffset)
			dest := src
			genSrcCmd := &GenerateCommand{
				Dest:          filepath.Join(srcBase, item, src),
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
				Dest:          filepath.Join(destBase, item, dest),
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

			untilOffset := -29 * whispertool.Minute

			copyCmd := &CopyCommand{
				SrcBase:     srcBase,
				SrcRelPath:  filepath.Join(item, src),
				DestBase:    destBase,
				DestRelPath: filepath.Join(item, dest),
				From:        0,
				Until:       now.Add(untilOffset),
				Now:         now,
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
				Until:       now.Add(untilOffset),
				Now:         now,
				ArchiveID:   ArchiveIDAll,
				TextOut:     "-",
			}
			if err = diffCmd.Execute(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
