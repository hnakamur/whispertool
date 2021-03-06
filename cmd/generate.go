package cmd

import (
	crand "crypto/rand"
	"encoding/binary"
	"flag"
	"io"
	"math/rand"
	"os"
	"time"

	"github.com/hnakamur/whispertool"
)

type GenerateCommand struct {
	Dest              string
	Perm              os.FileMode
	AggregationMethod whispertool.AggregationMethod
	XFilesFactor      float32
	ArchiveInfoList   whispertool.ArchiveInfoList
	RandMax           int
	Fill              bool
	TextOut           string
}

func (c *GenerateCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.Dest, "dest", "", "dest whisper filename (ex. dest.wsp)")
	c.Perm = os.FileMode(0644)
	fs.Var(&fileModeValue{m: &c.Perm}, "perm", "whisper file permission (octal)")

	fs.Var(&aggregationMethodValue{&c.AggregationMethod}, "agg-method", "aggregation method")
	fs.Var(&xFilesFactorValue{&c.XFilesFactor}, "x-files-factor", "xFilesFactor")
	fs.Var(&archiveInfoListValue{&c.ArchiveInfoList}, "retentions", "retentions definitions")

	fs.IntVar(&c.RandMax, "max", 100, "random max value for shortest retention unit")
	fs.BoolVar(&c.Fill, "fill", true, "fill with random data")

	fs.StringVar(&c.TextOut, "text-out", "", "text output of copying data. empty means no output, - means stdout, other means output file")

	fs.Parse(args)

	if c.AggregationMethod == 0 {
		return newRequiredOptionError(fs, "agg-method")
	}
	if c.ArchiveInfoList == nil {
		return newRequiredOptionError(fs, "retentions")
	}
	if c.Dest == "" {
		return newRequiredOptionError(fs, "dest")
	}

	return nil
}

func (c *GenerateCommand) Execute() error {
	return withTextOutWriter(c.TextOut, c.execute)
}

func (c *GenerateCommand) execute(tow io.Writer) (err error) {
	db, err := whispertool.Create(c.Dest, c.ArchiveInfoList, c.AggregationMethod, c.XFilesFactor)
	if err != nil {
		return err
	}
	defer db.Close()

	var ptsList PointsList
	if c.Fill {
		rnd := rand.New(rand.NewSource(newRandSeed()))
		now := whispertool.TimestampFromStdTime(time.Now())
		until := now
		ptsList = randomPointsList(c.ArchiveInfoList, rnd, c.RandMax, until, now)
		if err := updateFileDataWithPointsList(db, ptsList, now); err != nil {
			return err
		}
	}

	if err = printFileData(tow, db.Header(), ptsList, true); err != nil {
		return err
	}

	if err := db.Sync(); err != nil {
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

func randomPointsList(retentions []whispertool.ArchiveInfo, rnd *rand.Rand, rndMaxForHightestArchive int, until, now whispertool.Timestamp) PointsList {
	pointsList := make([]whispertool.Points, len(retentions))
	var highRet *whispertool.ArchiveInfo
	var highRndMax int
	var highPts []whispertool.Point
	for i := range retentions {
		r := &retentions[i]
		rndMax := rndMaxForHightestArchive * int(r.SecondsPerPoint()) / int(retentions[0].SecondsPerPoint())
		pointsList[i] = randomPoints(r, highRet, highPts, rnd, rndMax, highRndMax, until, now)

		highRndMax = rndMax
		highPts = pointsList[i]
		highRet = r
	}
	return pointsList
}

func randomPoints(r, highRet *whispertool.ArchiveInfo, highPts []whispertool.Point, rnd *rand.Rand, rndMax, highRndMax int, until, now whispertool.Timestamp) []whispertool.Point {
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

func randomValWithHighSum(t whispertool.Timestamp, rnd *rand.Rand, highRndMax int, r, highRet *whispertool.ArchiveInfo, highPts []whispertool.Point) whispertool.Value {
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

func updateFileDataWithPointsList(db *whispertool.Whisper, pointsList PointsList, now whispertool.Timestamp) error {
	for archiveID := range db.ArchiveInfoList() {
		if err := db.UpdatePointsForArchive(pointsList[archiveID], archiveID, now); err != nil {
			return err
		}
	}
	return nil
}
