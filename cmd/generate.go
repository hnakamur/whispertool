package cmd

import (
	crand "crypto/rand"
	"encoding/binary"
	"flag"
	"math/rand"
	"os"
	"time"

	"github.com/hnakamur/whispertool"
)

type GenerateCommand struct {
	Dest          string
	Perm          os.FileMode
	RetentionDefs string
	RandMax       int
	Fill          bool
	Now           whispertool.Timestamp
	TextOut       string
}

func (c *GenerateCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.Dest, "dest", "", "dest whisper filename (ex. dest.wsp).")
	c.Perm = os.FileMode(0644)
	fs.Var(&fileModeValue{m: &c.Perm}, "perm", "whisper file permission (octal)")

	fs.StringVar(&c.RetentionDefs, "retentions", "1m:2h,1h:2d,1d:30d", "retentions definitions.")
	fs.IntVar(&c.RandMax, "max", 100, "random max value for shortest retention unit.")
	fs.BoolVar(&c.Fill, "fill", true, "fill with random data.")

	fs.StringVar(&c.TextOut, "text-out", "", "text output of copying data. empty means no output, - means stdout, other means output file.")

	c.Now = whispertool.TimestampFromStdTime(time.Now())
	fs.Var(&timestampValue{t: &c.Now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")

	fs.Parse(args)

	if c.RetentionDefs == "" {
		return newRequiredOptionError(fs, "retentions")
	}
	if c.Dest == "" {
		return newRequiredOptionError(fs, "dest")
	}

	return nil
}

func (c *GenerateCommand) Execute() error {
	retentions, err := whispertool.ParseRetentions(c.RetentionDefs)
	if err != nil {
		return err
	}

	m := whispertool.NewMeta(whispertool.Sum, 0)
	d, err := whispertool.NewFileData(m, retentions)
	if err != nil {
		return err
	}

	var pointsList [][]whispertool.Point
	if c.Fill {
		rnd := rand.New(rand.NewSource(newRandSeed()))
		until := c.Now
		pointsList = randomPointsList(retentions, until, c.Now, rnd, c.RandMax)
		if err := updateFileDataWithPointsList(d, pointsList, c.Now); err != nil {
			return err
		}
	}

	if err = printFileData(c.TextOut, d, pointsList, true); err != nil {
		return err
	}

	if err := whispertool.WriteFile(c.Dest, d, c.Perm); err != nil {
		return err
	}
	return nil
}

func newRandSeed() int64 {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return time.Now().UnixNano()
	}
	return int64(binary.BigEndian.Uint64(b[:]))
}

func randomPointsList(retentions []whispertool.Retention, until, now whispertool.Timestamp, rnd *rand.Rand, rndMaxForHightestArchive int) [][]whispertool.Point {
	pointsList := make([][]whispertool.Point, len(retentions))
	var highRet *whispertool.Retention
	var highRndMax int
	var highPts []whispertool.Point
	for i := range retentions {
		r := &retentions[i]
		rndMax := rndMaxForHightestArchive * int(r.SecondsPerPoint()) / int(retentions[0].SecondsPerPoint())
		pointsList[i] = randomPoints(until, now, r, highRet, rnd, rndMax, highRndMax, highPts)

		highRndMax = rndMax
		highPts = pointsList[i]
		highRet = r
	}
	return pointsList
}

func randomPoints(until, now whispertool.Timestamp, r, highRet *whispertool.Retention, rnd *rand.Rand, rndMax, highRndMax int, highPts []whispertool.Point) []whispertool.Point {
	// adjust now and until for this archive
	step := r.SecondsPerPoint()
	thisNow := now.Truncate(step)
	thisUntil := until.Truncate(step)

	var thisHighStartTime whispertool.Timestamp
	if highPts != nil {
		highStartTime := highPts[0].Time
		if highStartTime < thisUntil {
			thisHighStartTime = highStartTime.Truncate(step)
		}
	}

	n := int((r.MaxRetention() - thisNow.Sub(thisUntil)) / r.SecondsPerPoint())
	points := make([]whispertool.Point, n)
	for i := 0; i < n; i++ {
		t := thisUntil.Add(-whispertool.Duration(n-1-i) * step * whispertool.Second)
		var v whispertool.Value
		if thisHighStartTime == 0 || t < thisHighStartTime {
			v = whispertool.Value(rnd.Intn(rndMax + 1))
		} else {
			v = randomValWithHighSum(t, rnd, highRndMax, r, highRet, highPts)
		}
		points[i] = whispertool.Point{
			Time:  t,
			Value: v,
		}
	}
	return points
}

func randomValWithHighSum(t whispertool.Timestamp, rnd *rand.Rand, highRndMax int, r, highRet *whispertool.Retention, highPts []whispertool.Point) whispertool.Value {
	step := r.SecondsPerPoint()

	v := whispertool.Value(0)
	for _, hp := range highPts {
		thisHighTime := hp.Time.Truncate(step)
		if thisHighTime < t {
			continue
		}
		if thisHighTime > t {
			break
		}
		v += hp.Value
	}

	if len(highPts) == 0 {
		return v
	}
	highStartTime := highPts[0].Time
	if t >= highStartTime {
		return v
	}
	n := int(highStartTime.Sub(t) / whispertool.Second / highRet.SecondsPerPoint())
	v2 := whispertool.Value(n * rnd.Intn(highRndMax+1))
	return v + v2
}

func updateFileDataWithPointsList(d *whispertool.FileData, pointsList [][]whispertool.Point, now whispertool.Timestamp) error {
	for retID := range d.Retentions() {
		if err := d.UpdatePointsForArchive(retID, pointsList[retID], now); err != nil {
			return err
		}
	}
	return nil
}

//func updateWhisperFile(db *whispertool.Whisper, pointsList [][]whispertool.Point, now whispertool.Timestamp) error {
//	if pointsList == nil {
//		return nil
//	}
//	for i := range db.Retentions {
//		err := db.UpdatePointsForArchive(i, pointsList[i], now)
//		if err != nil {
//			return err
//		}
//	}
//
//	if err := db.Sync(); err != nil {
//		return err
//	}
//	return nil
//}