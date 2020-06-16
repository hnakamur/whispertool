package whispertool

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/hnakamur/timestamp"
	"golang.org/x/sync/errgroup"
)

const RetIdAll = -1

var debug = os.Getenv("DEBUG") != ""
var errRetIdOufOfRange = errors.New("retention id is out of range")

func View(filename string, raw bool, now, from, until time.Time, retId int, showHeader bool) error {
	if raw {
		return viewRaw(filename, now, from, until, retId, showHeader)
	}
	return view(filename, now, from, until, retId, showHeader)
}

type whisperFileData struct {
	filename          string
	aggregationMethod AggregationMethod
	maxRetention      uint32
	xFilesFactor      float32
	retentions        []Retention
	tss               [][]Point
}

func readWhisperFile(filename string, now, from, until time.Time, retId int) (*whisperFileData, error) {
	p := NewBufferPool(os.Getpagesize())
	db, err := Open(filename, p)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	return readWhisperDB(db, now, from, until, retId, filename)
}

func readWhisperDB(db *Whisper, now, from, until time.Time, retId int, filename string) (*whisperFileData, error) {
	if debug {
		log.Printf("readWhisperDB start, filename=%s, from=%s, until=%s, retId=%d",
			filename, formatTime(from), formatTime(until), retId)
	}

	retentions := db.Retentions
	tss := make([][]Point, len(retentions))
	if retId == RetIdAll {
		var g errgroup.Group
		for i := range retentions {
			i := i
			g.Go(func() error {
				ts, err := readWhisperSingleArchive(db, now, from, until, i, filename)
				if err != nil {
					return err
				}
				tss[i] = ts
				return nil
			})
			if err := g.Wait(); err != nil {
				return nil, err
			}
		}
	} else if 0 <= retId && retId < len(retentions) {
		ts, err := readWhisperSingleArchive(db, now, from, until, retId, filename)
		if err != nil {
			return nil, err
		}
		tss[retId] = ts
	} else {
		return nil, errRetIdOufOfRange
	}

	return &whisperFileData{
		filename:          filename,
		aggregationMethod: db.Meta.AggregationMethod,
		maxRetention:      db.Meta.MaxRetention,
		xFilesFactor:      db.Meta.XFilesFactor,
		retentions:        retentions,
		tss:               tss,
	}, nil
}

func readWhisperSingleArchive(db *Whisper, now, from, until time.Time, retId int, filename string) ([]Point, error) {
	nowUnix := int64(now.Unix())
	fromUnix := int64(from.Unix())

	untilUnix := int64(until.Unix())
	if untilUnix > nowUnix {
		untilUnix = nowUnix
	}

	fetchFrom := fromUnix
	fetchUntil := untilUnix

	retentions := db.Retentions
	r := retentions[retId]
	minFrom := nowUnix - int64(r.MaxRetention())
	step := int64(r.SecondsPerPoint)

	if debug {
		log.Printf("readWhisperSingleArchive retId=%d, fromUnix=%s, untilUnix=%s, minFrom=%s",
			retId,
			formatTime(secondsToTime(int64(fromUnix))),
			formatTime(secondsToTime(int64(untilUnix))),
			formatTime(secondsToTime(int64(minFrom))))
	}
	if fetchFrom < minFrom {
		if debug {
			log.Printf("adjust fetchFrom to minFrom")
		}
		fetchFrom = minFrom
	} else {
		// NOTE: We need to adjust from and until by subtracting step
		// since step is added to from and until in
		// go-whisper archiveInfo.Interval method.
		// https://github.com/go-graphite/go-whisper/blob/e5e7d31ca75557a461f9883667028ddc44713481/whisper.go#L1411
		//
		// I suppose archiveInfo.Interval follows
		// __archive_fetch in original graphite-project/whisper.
		// https://github.com/graphite-project/whisper/blob/master/whisper.py#L970-L972
		// I asked why step is added at
		// https://answers.launchpad.net/graphite/+question/294817
		// but no answer from the person who only knows
		// the original reason.
		fetchFrom -= step
		fetchUntil -= step
		if debug {
			log.Printf("adjust time range by subtracting step, fetchFrom=%s",
				formatTime(secondsToTime(int64(fetchFrom))))
		}
	}

	if fetchUntil < fetchFrom {
		return nil, nil
	}

	exptectedPtsLen := (fetchUntil - fetchFrom) / step
	if exptectedPtsLen == 0 {
		exptectedPtsLen = 1
	}

	if debug {
		log.Printf("calling db.Fetch with fetchFrom=%s, fetchUntil=%s",
			formatTime(secondsToTime(int64(fetchFrom))),
			formatTime(secondsToTime(int64(fetchUntil))))
	}
	pts, err := db.FetchFromSpecifiedArchive(
		retId,
		timestamp.Second(fetchFrom),
		timestamp.Second(fetchUntil),
		timestamp.Second(nowUnix))
	if err != nil {
		return nil, err
	}
	if debug {
		for i, pt := range pts {
			log.Printf("i=%d, pt.Time=%s, pt.Value=%s",
				i,
				formatTime(secondsToTime(int64(pt.Time))),
				strconv.FormatFloat(pt.Value, 'f', -1, 64))
		}
	}
	return pts, nil
}

func view(filename string, now, from, until time.Time, retId int, showHeader bool) error {
	d, err := readWhisperFile(filename, now, from, until, retId)
	if err != nil {
		return err
	}

	return d.WriteTo(os.Stdout, showHeader)
}

func writeWhisperFileData(textOut string, d *whisperFileData, showHeader bool) error {
	if textOut == "" {
		return nil
	}

	if textOut == "-" {
		return d.WriteTo(os.Stdout, showHeader)
	}

	file, err := os.Create(textOut)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	if err = d.WriteTo(w, showHeader); err != nil {
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

func (d *whisperFileData) WriteTo(w io.Writer, showHeader bool) error {
	if showHeader {
		_, err := fmt.Fprintf(w, "aggMethod:%s\tmaxRetention:%s\txFilesFactor:%s\n",
			d.aggregationMethod,
			secondsToDuration(int64(d.maxRetention)),
			strconv.FormatFloat(float64(d.xFilesFactor), 'f', -1, 32))
		if err != nil {
			return err
		}

		for i, r := range d.retentions {
			_, err := fmt.Fprintf(w, "retentionDef:%d\tstep:%s\tnumberOfPoints:%d\toffset:%d\n",
				i,
				secondsToDuration(int64(r.SecondsPerPoint)),
				r.NumberOfPoints,
				r.Offset,
			)
			if err != nil {
				return err
			}
		}
	}
	return writeTimeSeriesForArchives(w, d.tss)
}

func writeTimeSeriesForArchives(w io.Writer, tss [][]Point) error {
	for i, ts := range tss {
		for _, p := range ts {
			_, err := fmt.Fprintf(w, "retId:%d\tt:%s\tval:%s\n",
				i,
				formatTime(secondsToTime(int64(p.Time))),
				strconv.FormatFloat(p.Value, 'f', -1, 64))
			if err != nil {
				return err
			}
		}
	}
	return nil
}
