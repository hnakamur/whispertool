package whispertool

import (
	crand "crypto/rand"
	"encoding/binary"
	"flag"
	"math/rand"
	"os"
	"time"
)

type GenerateCommand struct {
	Dest          string
	Perm          os.FileMode
	RetentionDefs string
	RandMax       int
	Fill          bool
	Now           Timestamp
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

	c.Now = TimestampFromStdTime(time.Now())
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
	retentions, err := ParseRetentions(c.RetentionDefs)
	if err != nil {
		return err
	}

	m := Meta{
		AggregationMethod: Sum,
		XFilesFactor:      0,
	}
	d, err := NewFileData(m, retentions)
	if err != nil {
		return err
	}

	var pointsList [][]Point
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

	if err := WriteFile(c.Dest, d, c.Perm); err != nil {
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

func randomPointsList(retentions []Retention, until, now Timestamp, rnd *rand.Rand, rndMaxForHightestArchive int) [][]Point {
	pointsList := make([][]Point, len(retentions))
	var highRet *Retention
	var highRndMax int
	var highPts []Point
	for i := range retentions {
		r := &retentions[i]
		rndMax := rndMaxForHightestArchive * int(r.SecondsPerPoint) / int(retentions[0].SecondsPerPoint)
		pointsList[i] = randomPoints(until, now, r, highRet, rnd, rndMax, highRndMax, highPts)

		highRndMax = rndMax
		highPts = pointsList[i]
		highRet = r
	}
	return pointsList
}

func randomPoints(until, now Timestamp, r, highRet *Retention, rnd *rand.Rand, rndMax, highRndMax int, highPts []Point) []Point {
	// adjust now and until for this archive
	step := r.SecondsPerPoint
	thisNow := now.Truncate(step)
	thisUntil := until.Truncate(step)

	var thisHighStartTime Timestamp
	if highPts != nil {
		highStartTime := highPts[0].Time
		if highStartTime < thisUntil {
			thisHighStartTime = highStartTime.Truncate(step)
		}
	}

	n := int((r.MaxRetention() - thisNow.Sub(thisUntil)) / r.SecondsPerPoint)
	points := make([]Point, n)
	for i := 0; i < n; i++ {
		t := thisUntil.Add(-Duration(n-1-i) * step * Second)
		var v Value
		if thisHighStartTime == 0 || t < thisHighStartTime {
			v = Value(rnd.Intn(rndMax + 1))
		} else {
			v = randomValWithHighSum(t, rnd, highRndMax, r, highRet, highPts)
		}
		points[i] = Point{
			Time:  t,
			Value: v,
		}
	}
	return points
}

func randomValWithHighSum(t Timestamp, rnd *rand.Rand, highRndMax int, r, highRet *Retention, highPts []Point) Value {
	step := r.SecondsPerPoint

	v := Value(0)
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
	n := int(highStartTime.Sub(t) / Second / highRet.SecondsPerPoint)
	v2 := Value(n * rnd.Intn(highRndMax+1))
	return v + v2
}

func updateFileDataWithPointsList(d *FileData, pointsList [][]Point, now Timestamp) error {
	for retID := range d.Retentions {
		if err := d.UpdatePointsForArchive(retID, pointsList[retID], now); err != nil {
			return err
		}
	}
	return nil
}

//func updateWhisperFile(db *Whisper, pointsList [][]Point, now Timestamp) error {
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
