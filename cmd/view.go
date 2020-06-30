package cmd

import (
	"bufio"
	"flag"
	"io"
	"os"
	"time"

	"github.com/hnakamur/whispertool"
)

const RetIDAll = -1

var debug = os.Getenv("DEBUG") != ""

type ViewCommand struct {
	Filename   string
	From       whispertool.Timestamp
	Until      whispertool.Timestamp
	Now        whispertool.Timestamp
	RetID      int
	ShowHeader bool
	TextOut    string
}

func (c *ViewCommand) Parse(fs *flag.FlagSet, args []string) error {
	c.Now = whispertool.TimestampFromStdTime(time.Now())
	c.Until = c.Now
	fs.Var(&timestampValue{t: &c.Now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.From}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.Until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")
	fs.IntVar(&c.RetID, "ret", RetIDAll, "retention ID to diff (-1 is all)")
	fs.StringVar(&c.TextOut, "text-out", "-", "text output of copying data. empty means no output, - means stdout, other means output file.")
	fs.BoolVar(&c.ShowHeader, "header", true, "whether or not to show header (metadata and reteions)")
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

func (c *ViewCommand) Execute() error {
	d, pointsList, err := readWhisperFile(c.Filename, c.RetID, c.From, c.Until, c.Now)
	if err != nil {
		return err
	}

	if err := printFileData(c.TextOut, d, pointsList, c.ShowHeader); err != nil {
		return err
	}
	return nil
}

func readWhisperFile(filename string, retID int, from, until, now whispertool.Timestamp) (*whispertool.Whisper, PointsList, error) {
	db, err := whispertool.Open(filename)
	if err != nil {
		return nil, nil, err
	}
	defer db.Close()

	pointsList, err := fetchPointsList(db, retID, from, until, now)
	if err != nil {
		return nil, nil, err
	}
	return db, pointsList, nil
}

func fetchPointsList(db *whispertool.Whisper, retID int, from, until, now whispertool.Timestamp) (PointsList, error) {
	pointsList := make([]whispertool.Points, len(db.Retentions()))
	if retID == RetIDAll {
		for i := range db.Retentions() {
			points, err := db.FetchFromArchive(i, from, until, now)
			if err != nil {
				return nil, err
			}
			pointsList[i] = points
		}
	} else if retID >= 0 && retID < len(db.Retentions()) {
		points, err := db.FetchFromArchive(retID, from, until, now)
		if err != nil {
			return nil, err
		}
		pointsList[retID] = points
	} else {
		return nil, whispertool.ErrRetentionIDOutOfRange
	}
	return pointsList, nil
}

func printFileData(textOut string, db *whispertool.Whisper, pointsList PointsList, showHeader bool) error {
	if textOut == "" {
		return nil
	}

	if textOut == "-" {
		return printHeaderAndPointsList(os.Stdout, db, pointsList, showHeader)
	}

	file, err := os.Create(textOut)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	if err := printHeaderAndPointsList(w, db, pointsList, showHeader); err != nil {
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

func printHeaderAndPointsList(w io.Writer, db *whispertool.Whisper, pointsList []whispertool.Points, showHeader bool) error {
	if showHeader {
		if err := db.PrintHeader(w); err != nil {
			return err
		}
	}
	if err := PointsList(pointsList).Print(w); err != nil {
		return err
	}
	return nil
}
