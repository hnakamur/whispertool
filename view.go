package whispertool

import (
	"bufio"
	"flag"
	"io"
	"os"
	"time"
)

const RetIDAll = -1

var debug = os.Getenv("DEBUG") != ""

type ViewCommand struct {
	Filename   string
	From       Timestamp
	Until      Timestamp
	Now        Timestamp
	RetID      int
	ShowHeader bool
	TextOut    string
}

func (c *ViewCommand) Parse(fs *flag.FlagSet, args []string) error {
	c.Now = TimestampFromStdTime(time.Now())
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
	d, pointsList, err := readWhisperFile(c.Filename, c.Now, c.From, c.Until, c.RetID)
	if err != nil {
		return err
	}

	if err := printFileData(c.TextOut, d, pointsList, c.ShowHeader); err != nil {
		return err
	}
	return nil
}

func readWhisperFile(filename string, now, from, until Timestamp, retID int) (*FileData, [][]Point, error) {
	d, err := ReadFile(filename)
	if err != nil {
		return nil, nil, err
	}

	pointsList, err := fetchPointsList(d, now, from, until, retID)
	if err != nil {
		return nil, nil, err
	}
	return d, pointsList, nil
}

func fetchPointsList(d *FileData, now, from, until Timestamp, retID int) ([][]Point, error) {
	pointsList := make([][]Point, len(d.Retentions))
	if retID == RetIDAll {
		for i := range d.Retentions {
			points, err := d.FetchFromArchive(i, from, until, now)
			if err != nil {
				return nil, err
			}
			pointsList[i] = points
		}
	} else if retID >= 0 && retID < len(d.Retentions) {
		points, err := d.FetchFromArchive(retID, from, until, now)
		if err != nil {
			return nil, err
		}
		pointsList[retID] = points
	} else {
		return nil, ErrRetentionIDOutOfRange
	}
	return pointsList, nil
}

func printFileData(textOut string, d *FileData, pointsList [][]Point, showHeader bool) error {
	if textOut == "" {
		return nil
	}

	if textOut == "-" {
		return printHeaderAndPointsList(os.Stdout, d, pointsList, showHeader)
	}

	file, err := os.Create(textOut)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	if err := printHeaderAndPointsList(w, d, pointsList, showHeader); err != nil {
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

func printHeaderAndPointsList(w io.Writer, d *FileData, pointsList [][]Point, showHeader bool) error {
	if showHeader {
		if err := d.PrintHeader(w); err != nil {
			return err
		}
	}
	if err := PointsList(pointsList).Print(w); err != nil {
		return err
	}
	return nil
}
