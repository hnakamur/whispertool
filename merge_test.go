package whispertool_test

import (
	"testing"
)

func TestMerge(t *testing.T) {
	//	tests := []struct {
	//		name      string
	//		fromFn    func(now time.Time) time.Time
	//		untilFn   func(now time.Time) time.Time
	//		emptyRate float64
	//		randMax   int
	//	}{
	//		{
	//			name:      "full_range_20%_empty_max_100",
	//			fromFn:    func(time.Time) time.Time { return time.Unix(0, 0) },
	//			untilFn:   func(now time.Time) time.Time { return now },
	//			emptyRate: 0.2,
	//			randMax:   100,
	//		},
	//		{
	//			name:      "full_range_20%_empty_max_0",
	//			fromFn:    func(time.Time) time.Time { return time.Unix(0, 0) },
	//			untilFn:   func(now time.Time) time.Time { return now },
	//			emptyRate: 0.2,
	//			randMax:   0,
	//		},
	//		{
	//			name:      "full_range_100%_empty_max_100",
	//			fromFn:    func(time.Time) time.Time { return time.Unix(0, 0) },
	//			untilFn:   func(now time.Time) time.Time { return now },
	//			emptyRate: 1,
	//			randMax:   100,
	//		},
	//		{
	//			name: "range_-60m_to_-30m_100%_empty_max_100",
	//			fromFn: func(now time.Time) time.Time {
	//				return now.Add(-60 * time.Minute)
	//			},
	//			untilFn: func(now time.Time) time.Time {
	//				return now.Add(-30 * time.Minute)
	//			},
	//			emptyRate: 1,
	//			randMax:   100,
	//		},
	//		{
	//			name: "range_-120m_to_-30m_100%_empty_max_100",
	//			fromFn: func(now time.Time) time.Time {
	//				return now.Add(-120 * time.Minute)
	//			},
	//			untilFn: func(now time.Time) time.Time {
	//				return now.Add(-30 * time.Minute)
	//			},
	//			emptyRate: 1,
	//			randMax:   100,
	//		},
	//		{
	//			name: "range_-150m_to_-30m_100%_empty_max_100",
	//			fromFn: func(now time.Time) time.Time {
	//				return now.Add(-150 * time.Minute)
	//			},
	//			untilFn: func(now time.Time) time.Time {
	//				return now.Add(-30 * time.Minute)
	//			},
	//			emptyRate: 1,
	//			randMax:   100,
	//		},
	//		{
	//			name: "range_-24h_to_-120m_100%_empty_max_100",
	//			fromFn: func(now time.Time) time.Time {
	//				return now.Add(-24 * time.Hour)
	//			},
	//			untilFn: func(now time.Time) time.Time {
	//				return now.Add(-120 * time.Minute)
	//			},
	//			emptyRate: 1,
	//			randMax:   100,
	//		},
	//		{
	//			name: "range_-36h_to_-120m_100%_empty_max_100",
	//			fromFn: func(now time.Time) time.Time {
	//				return now.Add(-36 * time.Hour)
	//			},
	//			untilFn: func(now time.Time) time.Time {
	//				return now.Add(-120 * time.Minute)
	//			},
	//			emptyRate: 1,
	//			randMax:   100,
	//		},
	//		{
	//			name: "range_-48h_to_-120m_100%_empty_max_100",
	//			fromFn: func(now time.Time) time.Time {
	//				return now.Add(-48 * time.Hour)
	//			},
	//			untilFn: func(now time.Time) time.Time {
	//				return now.Add(-120 * time.Minute)
	//			},
	//			emptyRate: 1,
	//			randMax:   100,
	//		},
	//		{
	//			name: "range_-72h_to_-120m_100%_empty_max_100",
	//			fromFn: func(now time.Time) time.Time {
	//				return now.Add(-72 * time.Hour)
	//			},
	//			untilFn: func(now time.Time) time.Time {
	//				return now.Add(-120 * time.Minute)
	//			},
	//			emptyRate: 1,
	//			randMax:   100,
	//		},
	//		{
	//			name: "range_-72h_to_-48h_100%_empty_max_100",
	//			fromFn: func(now time.Time) time.Time {
	//				return now.Add(-72 * time.Hour)
	//			},
	//			untilFn: func(now time.Time) time.Time {
	//				return now.Add(-48 * time.Hour)
	//			},
	//			emptyRate: 1,
	//			randMax:   100,
	//		},
	//	}
	//	for _, tc := range tests {
	//		tc := tc
	//		t.Run(tc.name, func(t *testing.T) {
	//			t.Parallel()
	//
	//			emptyRate := tc.emptyRate
	//			now := time.Now().UTC()
	//			from := tc.fromFn(now)
	//			until := tc.untilFn(now)
	//
	//			dir, err := ioutil.TempDir("", "whispertool_test")
	//			if err != nil {
	//				t.Fatal(err)
	//			}
	//			srcFilename := filepath.Join(dir, "src.wsp")
	//			destFilename := filepath.Join(dir, "dest.wsp")
	//			t.Cleanup(func() {
	//				if err == nil {
	//					os.RemoveAll(dir)
	//				} else {
	//					fmt.Printf("We left dir=%s for you to investigate. Please delete it yourself after investigation.  For convenience, here is command to see diff:\nwhispertool diff -from=%s -until=%s %s %s\n",
	//						dir,
	//						from.Format(whispertool.UTCTimeLayout),
	//						until.Format(whispertool.UTCTimeLayout),
	//						srcFilename,
	//						destFilename,
	//					)
	//				}
	//			})
	//
	//			const retentionDefs = "1m:2h,1h:2d,1d:30d"
	//			const fill = true
	//			randMax := tc.randMax
	//			err = whispertool.Generate(srcFilename, retentionDefs, fill, randMax, now, "")
	//			if err != nil {
	//				t.Fatal(err)
	//			}
	//
	//			err = whispertool.Hole(srcFilename, destFilename, emptyRate, now, from, until)
	//			if err != nil {
	//				t.Fatal(err)
	//			}
	//
	//			//err = errors.New("hoge")
	//			//t.Errorf("hoge")
	//			//const recursive = false
	//			//err = whispertool.Merge(srcFilename, destFilename, recursive, now, from, until)
	//			//if err != nil {
	//			//	t.Fatal(err)
	//			//}
	//
	//			//const ignoreSrcEmpty = false
	//			//const ignoreDestEmpty = false
	//			//const showAll = false
	//			//const retID = 0
	//			//err = whispertool.Diff(srcFilename, destFilename, recursive, ignoreSrcEmpty, ignoreDestEmpty, showAll, now, from, until, retID)
	//			//if err != nil {
	//			//	if errors.Is(err, whispertool.ErrDiffFound) {
	//			//		t.Errorf("merged result dest.wsp should have same content as src.wsp: %s", err)
	//			//	} else {
	//			//		t.Fatal(err)
	//			//	}
	//			//}
	//		})
	//	}
}
