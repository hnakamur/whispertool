package cmd

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/hnakamur/whispertool"
	"golang.org/x/sync/errgroup"
)

var ErrDiffFound = errors.New("diff found")

type DiffCommand struct {
	SrcURL  string
	Src     string
	Dest    string
	From    whispertool.Timestamp
	Until   whispertool.Timestamp
	Now     whispertool.Timestamp
	RetID   int
	TextOut string
}

func (c *DiffCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcURL, "src-url", "", "web app URL for src")
	fs.StringVar(&c.Src, "src", "", "glob pattern of source whisper files (ex. src/*.wsp).")
	fs.StringVar(&c.Dest, "dest", "", "dest whisper filename (ex. dest.wsp).")
	fs.IntVar(&c.RetID, "ret", RetIDAll, "retention ID to diff (-1 is all).")
	fs.StringVar(&c.TextOut, "text-out", "", "text output of copying data. empty means no output, - means stdout, other means output file.")

	c.Now = whispertool.TimestampFromStdTime(time.Now())
	c.Until = c.Now
	fs.Var(&timestampValue{t: &c.Now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.From}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.Until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")
	fs.Parse(args)

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

func (c *DiffCommand) Execute() error {
	var srcDB, destDB *whispertool.Whisper
	var srcPtsList, destPtsList [][]whispertool.Point
	var eg errgroup.Group
	eg.Go(func() error {
		var err error
		if c.SrcURL != "" {
			srcDB, srcPtsList, err = readWhisperFileRemote(c.SrcURL, c.Src, c.RetID, c.From, c.Until, c.Now)
			if err != nil {
				return err
			}
		} else {
			srcDB, srcPtsList, err = readWhisperFile(c.Src, c.RetID, c.From, c.Until, c.Now)
			if err != nil {
				return err
			}
		}
		return nil
	})
	eg.Go(func() error {
		var err error
		destDB, destPtsList, err = readWhisperFile(c.Dest, c.RetID, c.From, c.Until, c.Now)
		if err != nil {
			return err
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return err
	}

	if !whispertool.Retentions(srcDB.Retentions()).Equal(destDB.Retentions()) {
		return errors.New("retentions unmatch between src and dest whisper files")
	}

	srcPlDif, destPlDif := whispertool.PointsList(srcPtsList).Diff(destPtsList)
	if whispertool.PointsList(srcPlDif).AllEmpty() && whispertool.PointsList(destPlDif).AllEmpty() {
		return nil
	}

	err := printDiff(c.TextOut, srcDB, destDB, srcPtsList, destPtsList, srcPlDif, destPlDif)
	if err != nil {
		return err
	}

	return ErrDiffFound
}

func printDiff(textOut string, srcDB, destDB *whispertool.Whisper, srcPtsList, destPtsList, srcPlDif, destPlDif [][]whispertool.Point) error {
	if textOut == "" {
		return nil
	}

	if textOut == "-" {
		return printDiffTo(os.Stdout, srcDB, destDB, srcPtsList, destPtsList, srcPlDif, destPlDif)
	}

	file, err := os.Create(textOut)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	err = printDiffTo(w, srcDB, destDB, srcPtsList, destPtsList, srcPlDif, destPlDif)
	if err != nil {
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

func printDiffTo(w io.Writer, srcDB, destDB *whispertool.Whisper, srcPtsList, destPtsList, srcPlDif, destPlDif [][]whispertool.Point) error {
	for retID := range srcDB.Retentions() {
		srcPtsDif := srcPlDif[retID]
		destPtsDif := destPlDif[retID]
		for i, srcPt := range srcPtsDif {
			destPt := destPtsDif[i]
			fmt.Fprintf(w, "retID:%d\tt:%s\t\tsrcVal:%s\tdestVal:%s\tdestMinusSrc:%s\n",
				retID, srcPt.Time, srcPt.Value, destPt.Value, destPt.Value.Diff(srcPt.Value))

		}
	}
	return nil
}

func readWhisperFileRemote(srcURL, filename string, retID int, from, until, now whispertool.Timestamp) (*whispertool.Whisper, [][]whispertool.Point, error) {
	reqURL := fmt.Sprintf("%s/view?now=%s&file=%s",
		srcURL, url.QueryEscape(now.String()), url.QueryEscape(filename))
	db, err := getFileDataFromRemote(reqURL)
	if err != nil {
		return nil, nil, err
	}

	pointsList, err := fetchPointsList(db, retID, from, until, now)
	if err != nil {
		return nil, nil, err
	}
	return db, pointsList, nil
}

func getFileDataFromRemote(reqURL string) (*whispertool.Whisper, error) {
	resp, err := http.Get(reqURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	db, err := whispertool.Create("", nil, 0, 0, whispertool.WithInMemory(), whispertool.WithRawData(data))
	if err != nil {
		return nil, err
	}
	return db, nil
}
