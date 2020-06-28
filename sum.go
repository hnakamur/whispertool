package whispertool

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"
)

func RunSum(srcPattern, destFilename, textOut string, retID int, now, from, until time.Time) error {
	if destFilename != "" {
		return errors.New("writing sum to whisperfile is not implemented yet")
	}

	srcFilenames, err := filepath.Glob(srcPattern)
	if err != nil {
		return err
	}
	if len(srcFilenames) == 0 {
		return fmt.Errorf("no file matched for -src=%s", srcPattern)
	}

	tsNow := TimestampFromStdTime(now)
	tsFrom := TimestampFromStdTime(from)
	tsUntil := TimestampFromStdTime(until)
	d, ptsList, err := sumWhisperFile(srcFilenames, tsNow, tsFrom, tsUntil, retID)
	if err != nil {
		return err
	}

	showHeader := true
	if err := printFileData(textOut, d, ptsList, showHeader); err != nil {
		return err
	}
	return nil
}

func sumWhisperFile(srcFilenames []string, now, from, until Timestamp, retID int) (*FileData, [][]Point, error) {
	srcDataList := make([]*FileData, len(srcFilenames))
	ptsListList := make([][][]Point, len(srcFilenames))
	var g errgroup.Group
	for i, srcFilename := range srcFilenames {
		i := i
		srcFilename := srcFilename
		g.Go(func() error {
			d, ptsList, err := readWhisperFile(srcFilename, now, from, until, retID)
			if err != nil {
				return err
			}

			srcDataList[i] = d
			ptsListList[i] = ptsList
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	for i := 1; i < len(srcDataList); i++ {
		if !Retentions(srcDataList[0].Retentions).Equal(srcDataList[i].Retentions) {
			return nil, nil, fmt.Errorf("%s and %s archive confiugrations are unalike. "+
				"Resize the input before summing", srcFilenames[0], srcFilenames[i])
		}
	}

	sumData, err := NewFileData(srcDataList[0].Meta, srcDataList[0].Retentions)
	if err != nil {
		return nil, nil, err
	}

	sumPtsList := sumPointsLists(ptsListList)

	return sumData, sumPtsList, nil
}

func sumPointsLists(ptsListList [][][]Point) [][]Point {
	if len(ptsListList) == 0 {
		return nil
	}
	retentionCount := len(ptsListList[0])
	sumPtsList := make([][]Point, retentionCount)
	for retID := range sumPtsList {
		sumPtsList[retID] = sumPointsListsForRetention(ptsListList, retID)
	}
	return sumPtsList
}

func sumPointsListsForRetention(ptsListList [][][]Point, retID int) []Point {
	if len(ptsListList) == 0 {
		return nil
	}
	ptsCount := len(ptsListList[0][retID])
	sumPoints := make([]Point, ptsCount)
	for i := range ptsListList {
		for j := range sumPoints {
			if i == 0 {
				sumPoints[j] = ptsListList[i][retID][j]
			} else {
				sumPoints[j].Value = sumPoints[j].Value.Add(ptsListList[i][retID][j].Value)
			}
		}
	}
	return nil
}
