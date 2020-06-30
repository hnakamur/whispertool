package cmd

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/hnakamur/whispertool"
)

type HoleCommand struct {
	Src       string
	Dest      string
	Perm      os.FileMode
	EmptyRate float64
	From      whispertool.Timestamp
	Until     whispertool.Timestamp
	Now       whispertool.Timestamp
	RetID     int
	TextOut   string
}

func (c *HoleCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.Src, "src", "", "glob pattern of source whisper files (ex. src/*.wsp).")
	fs.StringVar(&c.Dest, "dest", "", "dest whisper filename (ex. dest.wsp).")
	fs.IntVar(&c.RetID, "ret", RetIDAll, "retention ID to diff (-1 is all).")
	fs.StringVar(&c.TextOut, "text-out", "", "text output of copying data. empty means no output, - means stdout, other means output file.")

	c.Now = whispertool.TimestampFromStdTime(time.Now())
	c.Until = c.Now
	fs.Var(&timestampValue{t: &c.Now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.From}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.Until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")

	c.Perm = os.FileMode(0644)
	fs.Var(&fileModeValue{m: &c.Perm}, "perm", "whisper file permission (octal)")
	fs.Float64Var(&c.EmptyRate, "empty-rate", 0.2, "empty rate (0 <= r <= 1).")
	fs.Parse(args)

	if c.EmptyRate < 0 || 1 < c.EmptyRate {
		return errEmptyRateOutOfBounds
	}
	if c.Src == "" {
		return newRequiredOptionError(fs, "src")
	}
	if c.Dest == "" {
		return newRequiredOptionError(fs, "dest")
	}
	if c.From > c.Until {
		return errFromIsAfterUntil
	}

	return nil
}

func (c *HoleCommand) Execute() error {
	srcDB, srcPtsList, err := readWhisperFile(c.Src, RetIDAll, c.From, c.Until, c.Now)
	if err != nil {
		return err
	}

	rnd := rand.New(rand.NewSource(newRandSeed()))
	destPtsList := emptyRandomPointsList(srcPtsList, rnd, c.EmptyRate, c.From, c.Until, srcDB.Retentions())
	var destDB *whispertool.Whisper
	if c.Dest != c.Src {
		destDB, err = whispertool.Create(c.Dest, srcDB.Retentions(), srcDB.AggregationMethod(), srcDB.XFilesFactor(), whispertool.WithFlock())
	} else {
		destDB, err = whispertool.Open(c.Dest, whispertool.WithFlock())
	}
	if err != nil {
		return err
	}
	defer destDB.Close()

	if err := updateFileDataWithPointsList(destDB, destPtsList, c.Now); err != nil {
		return err
	}

	if err = printFileData(c.TextOut, destDB, destPtsList, true); err != nil {
		return err
	}

	if err := destDB.Sync(); err != nil {
		return err
	}
	return nil
}

func emptyRandomPointsList(ptsList PointsList, rnd *rand.Rand, emptyRate float64, from, until whispertool.Timestamp, retentions []whispertool.Retention) PointsList {
	ptsList2 := make([]whispertool.Points, len(ptsList))
	for i, pts := range ptsList {
		r := &retentions[i]
		ptsList2[i] = emptyRandomPoints(pts, rnd, emptyRate, from, until, r)
	}
	return ptsList2
}

func emptyRandomPoints(pts []whispertool.Point, rnd *rand.Rand, empyRate float64, from, until whispertool.Timestamp, r *whispertool.Retention) []whispertool.Point {
	var pts2 []whispertool.Point
	for _, p := range pts {
		if from < p.Time && p.Time <= until && rnd.Float64() < empyRate {
			log.Printf("skip r=%s, p.Time=%s", r, p.Time)
			continue
		}
		pts2 = append(pts2, whispertool.Point{
			Time:  p.Time,
			Value: p.Value,
		})
	}
	return pts2
}
