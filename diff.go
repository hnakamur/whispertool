package whispertool

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"golang.org/x/sync/errgroup"
)

var ErrDiffFound = errors.New("diff found")

func Diff(src, dest string, textOut string, recursive, ignoreSrcEmpty, ignoreDestEmpty, showAll bool, now, from, until time.Time, retID int) error {
	if recursive {
		return errors.New("recursive option not implemented yet")
	}

	tsNow := TimestampFromStdTime(now)
	tsFrom := TimestampFromStdTime(from)
	tsUntil := TimestampFromStdTime(until)

	var srcData, destData *FileData
	var srcPtsList, destPtsList [][]Point
	var eg errgroup.Group
	eg.Go(func() error {
		var err error
		srcData, srcPtsList, err = readWhisperFile(src, tsNow, tsFrom, tsUntil, retID)
		if err != nil {
			return err
		}
		return nil
	})
	eg.Go(func() error {
		var err error
		destData, destPtsList, err = readWhisperFile(dest, tsNow, tsFrom, tsUntil, retID)
		if err != nil {
			return err
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return err
	}

	if !Retentions(srcData.Retentions).Equal(destData.Retentions) {
		return errors.New("retentions unmatch between src and dest whisper files")
	}

	srcPlDif, destPlDif := PointsList(srcPtsList).Diff(destPtsList)
	if PointsList(srcPlDif).AllEmpty() && PointsList(destPlDif).AllEmpty() {
		return nil
	}

	err := printDiff(textOut, srcData, destData, srcPtsList, destPtsList, srcPlDif, destPlDif, ignoreSrcEmpty, ignoreDestEmpty, showAll)
	if err != nil {
		return err
	}

	return ErrDiffFound
}

func printDiff(textOut string, srcData, destData *FileData, srcPtsList, destPtsList, srcPlDif, destPlDif [][]Point, ignoreSrcEmpty, ignoreDestEmpty, showAll bool) error {
	if textOut == "" {
		return nil
	}

	if textOut == "-" {
		return printDiffTo(os.Stdout, srcData, destData, srcPtsList, destPtsList, srcPlDif, destPlDif, ignoreSrcEmpty, ignoreDestEmpty, showAll)
	}

	file, err := os.Create(textOut)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	err = printDiffTo(w, srcData, destData, srcPtsList, destPtsList, srcPlDif, destPlDif, ignoreSrcEmpty, ignoreDestEmpty, showAll)
	if err != nil {
		return err
	}
	if err = w.Flush(); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	return nil
}

func printDiffTo(w io.Writer, srcData, destData *FileData, srcPtsList, destPtsList, srcPlDif, destPlDif [][]Point, ignoreSrcEmpty, ignoreDestEmpty, showAll bool) error {
	if showAll {
		for retID := range srcData.Retentions {
			srcPts := srcPtsList[retID]
			destPts := destPtsList[retID]
			for i, srcPt := range srcPts {
				destPt := destPts[i]
				var diff int
				if !srcPt.Equal(destPt) &&
					!(srcPt.Time == destPt.Time && ((ignoreSrcEmpty && srcPt.Value.IsNaN()) || (ignoreDestEmpty && destPt.Value.IsNaN()))) {
					diff = 1
				}
				fmt.Fprintf(w, "retID:%d\tt:%s\tsrcVal:%s\tdestVal:%s\tdiff:%d\n",
					retID, srcPt.Time, srcPt.Value, destPt.Value, diff)
			}
		}
	}

	for retID := range srcData.Retentions {
		srcPtsDif := srcPlDif[retID]
		destPtsDif := destPlDif[retID]
		for i, srcPt := range srcPtsDif {
			destPt := destPtsDif[i]
			if (ignoreSrcEmpty && srcPt.Value.IsNaN()) || (ignoreDestEmpty && destPt.Value.IsNaN()) {
				continue
			}
			fmt.Fprintf(w, "retID:%d\tt:%s\t\tsrcVal:%s\tdestVal:%s\n",
				retID, srcPt.Time, srcPt.Value, destPt.Value)

		}
	}
	return nil
}
