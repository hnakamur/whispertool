package whispertool

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func View(filename string, raw bool, now, from, until time.Time) error {
	if raw {
		return viewRaw(filename, now, from, until)
	}
	return view(filename, now, from, until)
}

type whisperFileData struct {
	aggMethod    string
	maxRetention int
	xFilesFactor float32
	retentions   []whisper.Retention
	tss          [][]*whisper.TimeSeriesPoint
}

func readWhisperFile(filename string, now, from, until time.Time) (*whisperFileData, error) {
	oflag := os.O_RDONLY
	opts := &whisper.Options{OpenFileFlag: &oflag}
	db, err := whisper.OpenWithOptions(filename, opts)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	return readWhisperDB(db, now, from, until)
}

func readWhisperDB(db *whisper.Whisper, now, from, until time.Time) (*whisperFileData, error) {
	//log.Printf("readWhisperDB start, from=%s, until=%s",
	//	formatTime(from), formatTime(until))
	nowUnix := int(now.Unix())
	fromUnix := int(from.Unix())

	untilUnix := int(until.Unix())
	if untilUnix > nowUnix {
		untilUnix = nowUnix
	}

	retentions := db.Retentions()
	tss := make([][]*whisper.TimeSeriesPoint, len(retentions))
	highMinFrom := nowUnix
	for i, r := range retentions {
		fetchFrom := fromUnix
		step := r.SecondsPerPoint()
		minFrom := nowUnix - r.MaxRetention()
		//log.Printf("retentionId=%d, fromUnix=%s, untilUnix=%s, minFrom=%s",
		//	i,
		//	formatTime(secondsToTime(int64(fromUnix))),
		//	formatTime(secondsToTime(int64(untilUnix))),
		//	formatTime(secondsToTime(int64(minFrom))))
		if fetchFrom < minFrom {
			//log.Printf("adjust fetchFrom to minFrom")
			fetchFrom = minFrom
		} else if highMinFrom <= fetchFrom {
			fetchFrom = int(alignUnixTime(int64(highMinFrom), step))
			if fetchFrom == highMinFrom {
				fetchFrom -= step
			}
			//log.Printf("adjust fetchFrom to %s",
			//	formatTime(secondsToTime(int64(fetchFrom))))
		}
		if fetchFrom <= untilUnix {
			//log.Printf("calling db.Fetch with fetchFrom=%s, untilUnix=%s",
			//	formatTime(secondsToTime(int64(fetchFrom))),
			//	formatTime(secondsToTime(int64(untilUnix))))
			ts, err := db.Fetch(fetchFrom, untilUnix)
			if err != nil {
				return nil, err
			}
			if fetchFrom < fromUnix {
				//log.Printf("calling filterTsPointPointersInRange with fromUnix=%s, untilUnix=%s",
				//	formatTime(secondsToTime(int64(fromUnix))),
				//	formatTime(secondsToTime(int64(untilUnix))))
				tss[i] = filterTsPointPointersInRange(ts.PointPointers(), fromUnix, untilUnix)
			} else {
				//log.Printf("use ts.PointPointers as is")
				tss[i] = ts.PointPointers()
			}
		}
		highMinFrom = minFrom
	}
	return &whisperFileData{
		aggMethod:    db.AggregationMethod(),
		maxRetention: db.MaxRetention(),
		xFilesFactor: db.XFilesFactor(),
		retentions:   retentions,
		tss:          tss,
	}, nil
}

func view(filename string, now, from, until time.Time) error {
	d, err := readWhisperFile(filename, now, from, until)
	if err != nil {
		return err
	}

	return d.WriteTo(os.Stdout)
}

func filterTsPointPointersInRange(pts []*whisper.TimeSeriesPoint, from, until int) []*whisper.TimeSeriesPoint {
	var pts2 []*whisper.TimeSeriesPoint
	for _, pt := range pts {
		if pt.Time < from {
			continue
		}
		if until < pt.Time {
			break
		}
		pts2 = append(pts2, pt)
	}
	return pts2
}

func writeWhisperFileData(textOut string, d *whisperFileData) error {
	if textOut == "" {
		return nil
	}

	if textOut == "-" {
		return d.WriteTo(os.Stdout)
	}

	file, err := os.Create(textOut)
	if err != nil {
		return err
	}
	defer file.Close()

	err = d.WriteTo(bufio.NewWriter(file))
	if err != nil {
		return err
	}

	err = file.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (d *whisperFileData) WriteTo(w io.Writer) error {
	_, err := fmt.Fprintf(w, "aggMethod:%s\tmaxRetention:%s\txFilesFactor:%s\n",
		d.aggMethod,
		secondsToDuration(int64(d.maxRetention)),
		strconv.FormatFloat(float64(d.xFilesFactor), 'f', -1, 32))
	if err != nil {
		return err
	}

	for i, r := range d.retentions {
		_, err := fmt.Fprintf(w, "retentionDef:%d\tstep:%s\tnumberOfPoints:%d\tsize:%d\n",
			i,
			secondsToDuration(int64(r.SecondsPerPoint())),
			r.NumberOfPoints(),
			r.Size(),
		)
		if err != nil {
			return err
		}
	}
	return writeTimeSeriesForArchives(w, d.tss)
}

func writeTimeSeriesForArchives(w io.Writer, tss [][]*whisper.TimeSeriesPoint) error {
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
