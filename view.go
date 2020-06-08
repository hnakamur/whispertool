package whispertool

import (
	"fmt"
	"os"
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
		if fetchFrom < minFrom {
			fetchFrom = minFrom
		} else if highMinFrom <= fetchFrom {
			fetchFrom = int(alignUnixTime(int64(highMinFrom), step))
			if fetchFrom == highMinFrom {
				fetchFrom -= step
			}
		}
		if fetchFrom <= untilUnix {
			ts, err := db.Fetch(fetchFrom, untilUnix)
			if err != nil {
				return nil, err
			}
			tss[i] = filterTsPointPointersInRange(ts.PointPointers(), fromUnix, untilUnix)
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

	fmt.Printf("aggMethod:%s\tmaxRetention:%s\txFilesFactor:%g\n",
		d.aggMethod,
		secondsToDuration(int64(d.maxRetention)),
		d.xFilesFactor)

	for i, r := range d.retentions {
		fmt.Printf("retentionDef:%d\tstep:%s\tnumberOfPoints:%d\tsize:%d\n",
			i,
			secondsToDuration(int64(r.SecondsPerPoint())),
			r.NumberOfPoints(),
			r.Size(),
		)
	}
	printTimeSeriesForArchives(d.tss)
	return nil
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

func printTimeSeriesForArchives(tss [][]*whisper.TimeSeriesPoint) {
	for i, ts := range tss {
		for _, p := range ts {
			fmt.Printf("retId:%d\tt:%s\tval:%g\n",
				i, formatTime(secondsToTime(int64(p.Time))), p.Value)
		}
	}
}
