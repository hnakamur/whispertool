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
	tests := []struct {
		name      string
		fromFn    func(now time.Time) time.Time
		untilFn   func(now time.Time) time.Time
		emptyRate float64
		randMax   int
	}{
		{
			name:      "full_range_20%_empty_max_100",
			fromFn:    func(time.Time) time.Time { return time.Unix(0, 0) },
			untilFn:   func(now time.Time) time.Time { return now },
			emptyRate: 0.2,
			randMax:   100,
		},
		{
			name:      "full_range_20%_empty_max_0",
			fromFn:    func(time.Time) time.Time { return time.Unix(0, 0) },
			untilFn:   func(now time.Time) time.Time { return now },
			emptyRate: 0.2,
			randMax:   0,
		},
		{
			name:      "full_range_100%_empty_max_100",
			fromFn:    func(time.Time) time.Time { return time.Unix(0, 0) },
			untilFn:   func(now time.Time) time.Time { return now },
			emptyRate: 1,
			randMax:   100,
		},
		{
			name: "range_-60m_to_-30m_100%_empty_max_100",
			fromFn: func(now time.Time) time.Time {
				return now.Add(-60 * time.Minute)
			},
			untilFn: func(now time.Time) time.Time {
				return now.Add(-30 * time.Minute)
			},
			emptyRate: 1,
			randMax:   100,
		},
		{
			name: "range_-120m_to_-30m_100%_empty_max_100",
			fromFn: func(now time.Time) time.Time {
				return now.Add(-120 * time.Minute)
			},
			untilFn: func(now time.Time) time.Time {
				return now.Add(-30 * time.Minute)
			},
			emptyRate: 1,
			randMax:   100,
		},
		{
			name: "range_-150m_to_-30m_100%_empty_max_100",
			fromFn: func(now time.Time) time.Time {
				return now.Add(-150 * time.Minute)
			},
			untilFn: func(now time.Time) time.Time {
				return now.Add(-30 * time.Minute)
			},
			emptyRate: 1,
			randMax:   100,
		},
		{
			name: "range_-24h_to_-120m_100%_empty_max_100",
			fromFn: func(now time.Time) time.Time {
				return now.Add(-24 * time.Hour)
			},
			untilFn: func(now time.Time) time.Time {
				return now.Add(-120 * time.Minute)
			},
			emptyRate: 1,
			randMax:   100,
		},
		{
			name: "range_-36h_to_-120m_100%_empty_max_100",
			fromFn: func(now time.Time) time.Time {
				return now.Add(-36 * time.Hour)
			},
			untilFn: func(now time.Time) time.Time {
				return now.Add(-120 * time.Minute)
			},
			emptyRate: 1,
			randMax:   100,
		},
		{
			name: "range_-48h_to_-120m_100%_empty_max_100",
			fromFn: func(now time.Time) time.Time {
				return now.Add(-48 * time.Hour)
			},
			untilFn: func(now time.Time) time.Time {
				return now.Add(-120 * time.Minute)
			},
			emptyRate: 1,
			randMax:   100,
		},
		{
			name: "range_-72h_to_-120m_100%_empty_max_100",
			fromFn: func(now time.Time) time.Time {
				return now.Add(-72 * time.Hour)
			},
			untilFn: func(now time.Time) time.Time {
				return now.Add(-120 * time.Minute)
			},
			emptyRate: 1,
			randMax:   100,
		},
		{
			name: "range_-72h_to_-48h_100%_empty_max_100",
			fromFn: func(now time.Time) time.Time {
				return now.Add(-72 * time.Hour)
			},
			untilFn: func(now time.Time) time.Time {
				return now.Add(-48 * time.Hour)
			},
			emptyRate: 1,
			randMax:   100,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

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
			randMax := tc.randMax
			err = whispertool.Generate(srcFilename, retentionDefs, fill, randMax)
			if err != nil {
				t.Fatal(err)
			}

			emptyRate := tc.emptyRate
			now := time.Now()
			from := tc.fromFn(now)
			until := tc.untilFn(now)
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
		})
	}
}