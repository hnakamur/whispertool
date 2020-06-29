package whispertool

import (
	"errors"
	"log"
	"time"

	"golang.org/x/sync/errgroup"
)

func Copy(srcURL, src, dest, textOut string, recursive bool, now, from, until time.Time, retID int) error {
	if recursive {
		return errors.New("recursive option not implemented yet")
	}

	tsNow := TimestampFromStdTime(now)
	tsFrom := TimestampFromStdTime(from)
	tsUntil := TimestampFromStdTime(until)
	log.Printf("copy, tsNow=%s, tsFrom=%s, tsUntil=%s", tsNow, tsFrom, tsUntil)

	var destDB *Whisper
	var srcData *FileData
	var srcPtsList [][]Point
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
		destDB, err = OpenForWrite(dest)
		if err != nil {
			return err
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return err
	}
	defer destDB.Close()

	destData := destDB.fileData
	if !Retentions(srcData.Retentions).Equal(destData.Retentions) {
		return errors.New("retentions unmatch between src and dest whisper files")
	}

	if err := updateFileDataWithPointsList(destData, srcPtsList, tsNow); err != nil {
		return err
	}

	if err := printFileData(textOut, srcData, srcPtsList, true); err != nil {
		return err
	}

	if err := destDB.Sync(); err != nil {
		return err
	}
	return nil
}
