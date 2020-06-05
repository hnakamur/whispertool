package whispertool

import (
	"fmt"
	"os"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func View(filename string, raw bool) error {
	if raw {
		return viewRaw(filename)
	}
	return view(filename)
}

type whisperFileData struct {
	aggMethod    string
	maxRetention int
	xFilesFactor float32
	retentions   []whisper.Retention
	tss          [][]*whisper.TimeSeriesPoint
}

func readWhisperFile(filename string, now time.Time) (*whisperFileData, error) {
	oflag := os.O_RDONLY
	opts := &whisper.Options{OpenFileFlag: &oflag}
	db, err := whisper.OpenWithOptions(filename, opts)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	untilTime := int(now.Unix())
	retentions := db.Retentions()
	tss := make([][]*whisper.TimeSeriesPoint, len(retentions))
	for i, r := range retentions {
		fromTime := untilTime - r.MaxRetention()
		ts, err := db.Fetch(fromTime, untilTime)
		if err != nil {
			return nil, err
		}
		tss[i] = ts.PointPointers()
	}
	return &whisperFileData{
		aggMethod:    db.AggregationMethod(),
		maxRetention: db.MaxRetention(),
		xFilesFactor: db.XFilesFactor(),
		retentions:   retentions,
		tss:          tss,
	}, nil
}

func view(filename string) error {
	d, err := readWhisperFile(filename, time.Now())
	if err != nil {
		return err
	}

	fmt.Printf("aggMethod:%s\tmaxRetention:%s\txFilesFactor:%g\n",
		d.aggMethod,
		secondsToDuration(int64(d.maxRetention)),
		d.xFilesFactor)

	for i, r := range d.retentions {
		fmt.Printf("retentionDef:%d\tretentionStep:%s\tnumberOfPoints:%d\tsize:%d\n",
			i,
			secondsToDuration(int64(r.SecondsPerPoint())),
			r.NumberOfPoints(),
			r.Size(),
		)
	}
	for i, ts := range d.tss {
		for j, p := range ts {
			fmt.Printf("retentionId:%d\tpointId:%d\ttime:%s\tvalue:%g\n",
				i, j, formatTime(secondsToTime(int64(p.Time))), p.Value)
		}
	}
	return nil
}
