package whispertool

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"golang.org/x/sync/errgroup"
)

var ErrDiffFound = errors.New("diff found")

func Diff(src, dest string, textOut string, recursive, ignoreSrcEmpty, ignoreDestEmpty, showAll bool, now, from, until time.Time, retID int, srcURL string) error {
	if recursive {
		return errors.New("recursive option not implemented yet")
	}
	log.Printf("Diff showAll=%v", showAll)

	tsNow := TimestampFromStdTime(now)
	tsFrom := TimestampFromStdTime(from)
	tsUntil := TimestampFromStdTime(until)

	var srcData, destData *FileData
	var srcPtsList, destPtsList [][]Point
	var eg errgroup.Group
	eg.Go(func() error {
		var err error
		if srcURL != "" {
			srcData, srcPtsList, err = readWhisperFileRemote(srcURL, src, tsNow, tsFrom, tsUntil, retID)
			if err != nil {
				return err
			}
		} else {
			srcData, srcPtsList, err = readWhisperFile(src, tsNow, tsFrom, tsUntil, retID)
			if err != nil {
				return err
			}
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

	// log.Printf("diff, srcPtsList.counts=%v, destPtsList.counts=%v", PointsList(srcPtsList).Counts(), PointsList(destPtsList).Counts())

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
				fmt.Fprintf(w, "retID:%d\tt:%s\tsrcVal:%s\tdestVal:%s\tdestMinusSrc:%s\tdiff:%d\n",
					retID, srcPt.Time, srcPt.Value, destPt.Value, destPt.Value.Diff(srcPt.Value), diff)
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
			fmt.Fprintf(w, "retID:%d\tt:%s\t\tsrcVal:%s\tdestVal:%s\tdestMinusSrc:%s\n",
				retID, srcPt.Time, srcPt.Value, destPt.Value, destPt.Value.Diff(srcPt.Value))

		}
	}
	return nil
}

func readWhisperFileRemote(srcURL, filename string, now, from, until Timestamp, retID int) (*FileData, [][]Point, error) {
	reqURL := fmt.Sprintf("%s/view?now=%s&file=%s",
		srcURL, url.QueryEscape(now.String()), url.QueryEscape(filename))
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

func getFileDataFromRemoteHelper(reqURL string) (*FileData, error) {
	resp, err := http.Get(reqURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	d, err := NewFileDataRead(data)
	if err != nil {
		return nil, err
	}
	return d, nil
}
