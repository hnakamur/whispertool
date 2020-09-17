package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hnakamur/whispertool"
	"golang.org/x/sync/errgroup"
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
	archiveInfoList, err := whispertool.ParseArchiveInfoList("1m:30h,1h:32d,1d:400d")
	if err != nil {
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

			item := fmt.Sprintf("item_%s", untilOffset)
			if err := os.MkdirAll(filepath.Join(srcBase, item), 0700); err != nil {
				t.Fatal(err)
			}

			if err := os.MkdirAll(filepath.Join(destBase, item), 0700); err != nil {
				t.Fatal(err)
			}

			src := "sv01.wsp"
			dest := src

			var eg errgroup.Group
			eg.Go(func() error {
				genSrcCmd := &GenerateCommand{
					Dest:              filepath.Join(srcBase, item, src),
					Perm:              0644,
					ArchiveInfoList:   archiveInfoList,
					AggregationMethod: whispertool.Sum,
					XFilesFactor:      0.0,
					RandMax:           1000,
					Fill:              true,
					TextOut:           "",
				}
				return genSrcCmd.Execute()
			})
			eg.Go(func() error {
				genDestCmd := &GenerateCommand{
					Dest:              filepath.Join(destBase, item, dest),
					Perm:              0644,
					ArchiveInfoList:   archiveInfoList,
					AggregationMethod: whispertool.Sum,
					XFilesFactor:      0.0,
					RandMax:           1000,
					Fill:              true,
					TextOut:           "",
				}
				return genDestCmd.Execute()
			})
			if err := eg.Wait(); err != nil {
				t.Fatal(err)
			}

			copyCmd := &CopyCommand{
				SrcBase:           srcBase,
				SrcRelPath:        filepath.Join(item, src),
				DestBase:          destBase,
				DestRelPath:       filepath.Join(item, dest),
				ArchiveInfoList:   archiveInfoList,
				AggregationMethod: whispertool.Sum,
				XFilesFactor:      0.0,
				From:              0,
				Until:             now.Add(untilOffset),
				ArchiveID:         ArchiveIDAll,
				TextOut:           "",
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
				ArchiveID:   ArchiveIDAll,
				TextOut:     "-",
			}
			if err = diffCmd.Execute(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCopyCommandMultiFiles(t *testing.T) {
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
	archiveInfoList, err := whispertool.ParseArchiveInfoList("1m:30h,1h:32d,1d:400d")
	if err != nil {
		t.Fatal(err)
	}

	now := whispertool.TimestampFromStdTime(time.Now())
	item := "item01"

	if err := os.MkdirAll(filepath.Join(srcBase, item), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(destBase, item), 0755); err != nil {
		t.Fatal(err)
	}

	var eg errgroup.Group
	const srcFileCount = 10
	for i := 0; i < srcFileCount; i++ {
		src := fmt.Sprintf("sv%02d.wsp", i)
		eg.Go(func() error {
			genSrcCmd := &GenerateCommand{
				Dest:              filepath.Join(srcBase, item, src),
				Perm:              0644,
				ArchiveInfoList:   archiveInfoList,
				AggregationMethod: whispertool.Sum,
				XFilesFactor:      0.0,
				RandMax:           1000,
				Fill:              true,
				TextOut:           "",
			}
			return genSrcCmd.Execute()
		})
	}

	const destFileCount = 5
	for i := 0; i < destFileCount; i++ {
		src := fmt.Sprintf("sv%02d.wsp", i)
		eg.Go(func() error {
			genDestCmd := &GenerateCommand{
				Dest:              filepath.Join(destBase, item, src),
				Perm:              0644,
				ArchiveInfoList:   archiveInfoList,
				AggregationMethod: whispertool.Sum,
				XFilesFactor:      0.0,
				RandMax:           1000,
				Fill:              true,
				TextOut:           "",
			}
			return genDestCmd.Execute()
		})
	}
	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}

	untilOffset := whispertool.Duration(0)
	copyCmd := &CopyCommand{
		SrcBase:           srcBase,
		SrcRelPath:        filepath.Join(item, "sv*.wsp"),
		DestBase:          destBase,
		DestRelPath:       "",
		ArchiveInfoList:   archiveInfoList,
		AggregationMethod: whispertool.Sum,
		XFilesFactor:      0.0,
		From:              0,
		Until:             now.Add(untilOffset),
		ArchiveID:         ArchiveIDAll,
		TextOut:           "",
	}
	if err = copyCmd.Execute(); err != nil {
		t.Fatal(err)
	}

	diffCmd := &DiffCommand{
		SrcBase:     srcBase,
		SrcRelPath:  filepath.Join(item, "sv*.wsp"),
		DestBase:    destBase,
		DestRelPath: "",
		From:        0,
		Until:       now.Add(untilOffset),
		ArchiveID:   ArchiveIDAll,
		TextOut:     "-",
	}
	if err = diffCmd.Execute(); err != nil {
		t.Fatal(err)
	}
}
