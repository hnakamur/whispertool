package whispertool

import (
	"bufio"
	"io"
	"os"
	"time"
)

const RetIdAll = -1

var debug = os.Getenv("DEBUG") != ""

func View(filename string, now, from, until time.Time, retId int, textOut string, showHeader bool) error {
	tsNow := TimestampFromStdTime(now)
	tsFrom := TimestampFromStdTime(from)
	tsUntil := TimestampFromStdTime(until)

	d, pointsList, err := readWhisperFile(filename, tsNow, tsFrom, tsUntil, retId)
	if err != nil {
		return err
	}

	if err := printFileData(textOut, d, pointsList, showHeader); err != nil {
		return err
	}
	return nil
}

func readWhisperFile(filename string, now, from, until Timestamp, retID int) (*FileData, [][]Point, error) {
	d, err := ReadFile(filename)
	if err != nil {
		return nil, nil, err
	}

	pointsList := make([][]Point, len(d.Retentions))
	if retID == RetIdAll {
		for i := range d.Retentions {
			points, err := d.FetchFromArchive(i, from, until, now)
			if err != nil {
				return nil, nil, err
			}
			pointsList[i] = points
		}
	} else if retID >= 0 && retID < len(d.Retentions) {
		points, err := d.FetchFromArchive(retID, from, until, now)
		if err != nil {
			return nil, nil, err
		}
		pointsList[retID] = points
	} else {
		return nil, nil, ErrRetentionIDOutOfRange
	}
	return d, pointsList, nil
}

func printFileData(textOut string, d *FileData, pointsList [][]Point, showHeader bool) error {
	if textOut == "" {
		return nil
	}

	if textOut == "-" {
		return printHeaderAndPointsList(os.Stdout, d, pointsList, showHeader)
	}

	file, err := os.Create(textOut)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	if err := printHeaderAndPointsList(w, d, pointsList, showHeader); err != nil {
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

func printHeaderAndPointsList(w io.Writer, d *FileData, pointsList [][]Point, showHeader bool) error {
	if showHeader {
		if err := d.PrintHeader(w); err != nil {
			return err
		}
	}
	if err := PointsList(pointsList).Print(w); err != nil {
		return err
	}
	return nil
}
