package whispertool

import (
	"bufio"
	"os"
	"time"
)

const RetIdAll = -1

var debug = os.Getenv("DEBUG") != ""

func View(filename string, raw bool, now, from, until time.Time, retId int, showHeader bool) error {
	if raw {
		return viewRaw(filename, from, until, retId, showHeader)
	}
	tsNow := TimestampFromStdTime(now)
	tsFrom := TimestampFromStdTime(from)
	tsUntil := TimestampFromStdTime(until)
	return view(filename, tsNow, tsFrom, tsUntil, retId, showHeader)
}

func readWhisperFile(filename string, now, from, until Timestamp, retID int) (*FileData, error) {
	d, err := ReadFile(filename)
	if err != nil {
		return nil, err
	}

	d.PointsList = make([][]Point, len(d.Retentions))
	if retID == RetIdAll {
		for i := range d.Retentions {
			points, err := d.FetchFromArchive(i, from, until, now)
			if err != nil {
				return nil, err
			}
			d.PointsList[i] = points
		}
	} else if retID >= 0 && retID < len(d.Retentions) {
		points, err := d.FetchFromArchive(retID, from, until, now)
		if err != nil {
			return nil, err
		}
		d.PointsList[retID] = points
	} else {
		return nil, ErrRetentionIDOutOfRange
	}
	return d, nil
}

func view(filename string, now, from, until Timestamp, retId int, showHeader bool) error {
	d, err := readWhisperFile(filename, now, from, until, retId)
	if err != nil {
		return err
	}

	return d.Print(os.Stdout, showHeader)
}

func writeWhisperFileData(textOut string, d *FileData, showHeader bool) error {
	if textOut == "" {
		return nil
	}

	if textOut == "-" {
		return d.Print(os.Stdout, showHeader)
	}

	file, err := os.Create(textOut)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	if err = d.Print(w, showHeader); err != nil {
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
