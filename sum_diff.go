package whispertool

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"
)

func SumDiff(srcURL, srcBase, destBase, itemPattern, srcPattern, dest, textOut string, ignoreSrcEmpty, ignoreDestEmpty, showAll bool, interval, intervalOffset, untilOffset time.Duration, retID int, from time.Time) error {
	//log.Printf("SumDiff start srcBase=%s, destBase=%s, itemPattern=%s, srcPattern=%s, dest=%s", srcBase, destBase, itemPattern, srcPattern, dest)
	if interval == 0 {
		err := sumDiffOneTime(srcURL, srcBase, destBase, itemPattern, srcPattern, dest, textOut, ignoreSrcEmpty, ignoreDestEmpty, showAll, untilOffset, retID, from)
		if err != nil {
			return err
		}
		return nil
	}

	for {
		now := time.Now()
		targetTime := now.Truncate(interval).Add(interval).Add(intervalOffset)
		time.Sleep(targetTime.Sub(now))

		err := sumDiffOneTime(srcURL, srcBase, destBase, itemPattern, srcPattern, dest, textOut, ignoreSrcEmpty, ignoreDestEmpty, showAll, untilOffset, retID, from)
		if err != nil {
			return err
		}
	}
	return nil
}

func sumDiffOneTime(srcURL, srcBase, destBase, itemPattern, srcPattern, dest, textOut string, ignoreSrcEmpty, ignoreDestEmpty, showAll bool, untilOffset time.Duration, retID int, from time.Time) error {
	t0 := time.Now()
	fmt.Printf("time:%s\tmsg:start\n", formatTime(t0))
	var totalItemCount int
	defer func() {
		t1 := time.Now()
		fmt.Printf("time:%s\tmsg:finish\tduration:%s\ttotalItemCount:%d\n", formatTime(t1), t1.Sub(t0).String(), totalItemCount)
	}()

	itemDirnames, err := filepath.Glob(filepath.Join(destBase, itemPattern))
	if err != nil {
		return err
	}
	if len(itemDirnames) == 0 {
		return fmt.Errorf("itemPattern match no directries, itemPattern=%s", itemPattern)
	}
	totalItemCount = len(itemDirnames)

	for _, itemDirname := range itemDirnames {
		itemRelDir, err := filepath.Rel(destBase, itemDirname)
		if err != nil {
			return err
		}
		fmt.Printf("item:%s\n", itemRelDir)
		err = sumDiffItem(srcURL, srcBase, destBase, itemRelDir, srcPattern, dest, textOut, ignoreSrcEmpty, ignoreDestEmpty, showAll, untilOffset, retID, from)
		if err != nil {
			return err
		}
	}
	return nil
}

func formatTime(t time.Time) string {
	return t.Format(UTCTimeLayout)
}

func sumDiffItem(srcURL, srcBase, destBase, itemRelDir, srcPattern, dest, textOut string, ignoreSrcEmpty, ignoreDestEmpty, showAll bool, untilOffset time.Duration, retID int, from time.Time) error {
	srcFullPattern := filepath.Join(srcBase, itemRelDir, srcPattern)
	var srcFilenames []string
	if srcURL == "" {
		srcFilenames, err := filepath.Glob(srcFullPattern)
		if err != nil {
			return err
		}
		if len(srcFilenames) == 0 {
			return fmt.Errorf("no file matched for -src=%s", srcPattern)
		}
	}
	destFull := filepath.Join(destBase, itemRelDir, dest)

	now := time.Now()
	until := now.Add(-untilOffset)
	tsNow := TimestampFromStdTime(now)
	tsFrom := TimestampFromStdTime(from)
	tsUntil := TimestampFromStdTime(until)

	var sumData, destData *FileData
	var sumPtsList, destPtsList [][]Point
	var g errgroup.Group
	g.Go(func() error {
		var err error
		if srcURL != "" {
			sumData, sumPtsList, err = sumWhisperFileRemote(srcURL, srcFullPattern, tsNow, tsFrom, tsUntil, retID)
			if err != nil {
				return err
			}
		} else {
			sumData, sumPtsList, err = sumWhisperFile(srcFilenames, tsNow, tsFrom, tsUntil, retID)
			if err != nil {
				return err
			}
		}
		return nil
	})
	g.Go(func() error {
		var err error
		destData, destPtsList, err = readWhisperFile(destFull, tsNow, tsFrom, tsUntil, retID)
		if err != nil {
			return err
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}

	if !Retentions(sumData.Retentions).Equal(destData.Retentions) {
		return errors.New("retentions unmatch between src and dest whisper files")
	}

	sumPlDif, destPlDif := PointsList(sumPtsList).Diff(destPtsList)
	if PointsList(sumPtsList).AllEmpty() && PointsList(destPlDif).AllEmpty() {
		return nil
	}

	err := printDiff(textOut, sumData, destData, sumPtsList, destPtsList, sumPlDif, destPlDif, ignoreSrcEmpty, ignoreDestEmpty, showAll)
	if err != nil {
		return err
	}
	return nil
}

func sumWhisperFileRemote(srcURL, srcFullPattern string, now, from, until Timestamp, retID int) (*FileData, [][]Point, error) {
	reqURL := fmt.Sprintf("%s/sum?now=%s&pattern=%s",
		srcURL, url.QueryEscape(now.String()), url.QueryEscape(srcFullPattern))
	d, err := getFileDataFromRemoteHelper(reqURL)
	if err != nil {
		return nil, nil, err
	}

	pointsList, err := fetchPointsList(d, now, from, until, retID)
	if err != nil {
		return nil, nil, err
	}
	return d, pointsList, nil
}
