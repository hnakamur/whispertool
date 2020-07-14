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

func TestSumCopyCommand(t *testing.T) {
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

	const srcServerCount = 40
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

			dest := "sum.wsp"

			var eg errgroup.Group
			for i := 0; i < srcServerCount; i++ {
				i := i
				eg.Go(func() error {
					src := fmt.Sprintf("sv%02d.wsp", i+1)
					genSrcCmd := &GenerateCommand{
						Dest:              filepath.Join(srcBase, item, src),
						Perm:              0644,
						ArchiveInfoList:   archiveInfoList,
						AggregationMethod: whispertool.Sum,
						XFilesFactor:      0.0,
						RandMax:           1000,
						Fill:              true,
						Now:               now,
						TextOut:           "",
					}
					return genSrcCmd.Execute()
				})
			}
			eg.Go(func() error {
				genDestCmd := &GenerateCommand{
					Dest:              filepath.Join(destBase, item, dest),
					Perm:              0644,
					ArchiveInfoList:   archiveInfoList,
					AggregationMethod: whispertool.Sum,
					XFilesFactor:      0.0,
					RandMax:           1000,
					Fill:              true,
					Now:               now,
					TextOut:           "",
				}
				return genDestCmd.Execute()
			})
			if err := eg.Wait(); err != nil {
				t.Fatal(err)
			}

			sumCopyCmd := &SumCopyCommand{
				SrcBase:     srcBase,
				ItemPattern: item,
				SrcPattern:  "sv*.wsp",
				DestBase:    destBase,
				DestRelPath: dest,
				From:        0,
				Until:       now.Add(untilOffset),
				Now:         now,
				ArchiveID:   ArchiveIDAll,
				TextOut:     "",
			}
			if err = sumCopyCmd.Execute(); err != nil {
				t.Fatal(err)
			}

			sumDiffCmd := &SumDiffCommand{
				SrcBase:     srcBase,
				ItemPattern: item,
				SrcPattern:  "sv*.wsp",
				DestBase:    destBase,
				DestRelPath: dest,
				From:        0,
				Until:       now.Add(untilOffset),
				Now:         now,
				ArchiveID:   ArchiveIDAll,
				TextOut:     "",
			}
			if err = sumDiffCmd.Execute(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
