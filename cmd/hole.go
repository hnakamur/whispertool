package cmd

import (
	"errors"
	"flag"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/hnakamur/whispertool"
)

type HoleCommand struct {
	SrcBase     string
	SrcRelPath  string
	DestBase    string
	DestRelPath string
	Perm        os.FileMode
	EmptyRate   float64
	From        whispertool.Timestamp
	Until       whispertool.Timestamp
	Now         whispertool.Timestamp
	RetID       int
	TextOut     string
}

func (c *HoleCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcBase, "src-base", "", "src base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.SrcRelPath, "src", "", "whisper file relative path to src base")
	fs.StringVar(&c.DestBase, "dest-base", "", "dest base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.DestRelPath, "dest", "", "whisper file relative path to dest base")
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

	if c.SrcBase == "" {
		return newRequiredOptionError(fs, "src-base")
	}
	if c.SrcRelPath == "" {
		return newRequiredOptionError(fs, "src")
	}
	if c.DestBase == "" {
		return newRequiredOptionError(fs, "dest-base")
	}
	if c.DestRelPath == "" {
		return newRequiredOptionError(fs, "dest")
	}
	if isBaseURL(c.DestBase) {
		return errors.New("not implemented yet for remote destination, currently only local destination is supported")
	}
	if c.EmptyRate < 0 || 1 < c.EmptyRate {
		return errEmptyRateOutOfBounds
	}
	if c.From > c.Until {
		return errFromIsAfterUntil
	}

	return nil
}

func (c *HoleCommand) Execute() error {
	srcDB, srcPtsList, err := readWhisperFile(c.SrcBase, c.SrcRelPath, c.RetID, c.From, c.Until, c.Now)
	if err != nil {
		return err
	}

	rnd := rand.New(rand.NewSource(newRandSeed()))
	destPtsList := emptyRandomPointsList(srcPtsList, rnd, c.EmptyRate, c.From, c.Until, srcDB.Retentions())

	destFullPath := filepath.Join(c.DestBase, c.DestRelPath)
	destDB, err := openOrCreateCopyDestFile(destFullPath, srcDB)
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
