package cmd

import (
	"flag"
	"sort"
	"time"

	"github.com/hnakamur/whispertool"
)

type ViewRawCommand struct {
	Filename    string
	From        whispertool.Timestamp
	Until       whispertool.Timestamp
	RetID       int
	ShowHeader  bool
	SortsByTime bool
	TextOut     string
}

func (c *ViewRawCommand) Parse(fs *flag.FlagSet, args []string) error {
	c.Until = whispertool.TimestampFromStdTime(time.Now())
	fs.Var(&timestampValue{t: &c.From}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.Until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")
	fs.IntVar(&c.RetID, "ret", RetIDAll, "retention ID to diff (-1 is all)")
	fs.BoolVar(&c.ShowHeader, "header", true, "whether or not to show header (metadata and reteions)")
	fs.BoolVar(&c.SortsByTime, "sort", false, "whether or not to sorts points by time")
	fs.StringVar(&c.TextOut, "text-out", "-", "text output of copying data. empty means no output, - means stdout, other means output file.")
	fs.Parse(args)

	if fs.NArg() != 1 {
		return errNeedsOneFileArg
	}
	c.Filename = fs.Arg(0)

	if c.From > c.Until {
		return errFromIsAfterUntil
	}

	return nil
}

func (c *ViewRawCommand) Execute() error {
	d, pointsList, err := readWhisperFileRaw(c.Filename, c.RetID)
	if err != nil {
		return err
	}

	pointsList = filterPointsListByTimeRange(d, pointsList, c.From, c.Until)
	if c.SortsByTime {
		sortPointsListByTime(pointsList)
	}

	if err := printFileData(c.TextOut, d, pointsList, c.ShowHeader); err != nil {
		return err
	}
	return nil
}

func readWhisperFileRaw(filename string, retID int) (*whispertool.FileData, [][]whispertool.Point, error) {
	d, err := whispertool.ReadFile(filename)
	if err != nil {
		return nil, nil, err
	}

	pointsList := make([][]whispertool.Point, len(d.Retentions()))
	if retID == RetIDAll {
		for i := range d.Retentions() {
			pointsList[i] = d.GetAllRawUnsortedPoints(i)
		}
	} else if retID >= 0 && retID < len(d.Retentions()) {
		pointsList[retID] = d.GetAllRawUnsortedPoints(retID)
	} else {
		return nil, nil, whispertool.ErrRetentionIDOutOfRange
	}
	return d, pointsList, nil
}

func filterPointsListByTimeRange(d *whispertool.FileData, pointsList [][]whispertool.Point, from, until whispertool.Timestamp) [][]whispertool.Point {
	pointsList2 := make([][]whispertool.Point, len(pointsList))
	for i := range d.Retentions() {
		r := &d.Retentions()[i]
		pointsList2[i] = filterPointsByTimeRange(r, pointsList[i], from, until)
	}
	return pointsList2
}

func filterPointsByTimeRange(r *whispertool.Retention, points []whispertool.Point, from, until whispertool.Timestamp) []whispertool.Point {
	if until == from {
		until = until.Add(r.SecondsPerPoint())
	}
	var points2 []whispertool.Point
	for _, p := range points {
		if (from != 0 && p.Time <= from) || p.Time > until {
			continue
		}
		points2 = append(points2, p)
	}
	return points2
}

func sortPointsListByTime(pointsList [][]whispertool.Point) {
	for _, points := range pointsList {
		sort.Stable(whispertool.Points(points))
	}
}
