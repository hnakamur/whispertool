package whispertool

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

	"golang.org/x/sync/errgroup"
)

var ErrDiffFound = errors.New("diff found")

type DiffCommand struct {
	SrcURL          string
	Src             string
	Dest            string
	From            Timestamp
	Until           Timestamp
	Now             Timestamp
	RetID           int
	TextOut         string
	IgnoreSrcEmpty  bool
	IgnoreDestEmpty bool
	ShowAll         bool
}

func (c *DiffCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcURL, "src-url", "", "web app URL for src")
	fs.StringVar(&c.Src, "src", "", "glob pattern of source whisper files (ex. src/*.wsp).")
	fs.StringVar(&c.Dest, "dest", "", "dest whisper filename (ex. dest.wsp).")
	fs.IntVar(&c.RetID, "ret", RetIDAll, "retention ID to diff (-1 is all).")
	fs.StringVar(&c.TextOut, "text-out", "", "text output of copying data. empty means no output, - means stdout, other means output file.")

	c.Now = TimestampFromStdTime(time.Now())
	c.Until = c.Now
	fs.Var(&timestampValue{t: &c.Now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.From}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.Until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")

	fs.BoolVar(&c.IgnoreSrcEmpty, "ignore-src-empty", false, "ignore diff when source point is empty.")
	fs.BoolVar(&c.IgnoreDestEmpty, "ignore-dest-empty", false, "ignore diff when destination point is empty.")
	fs.BoolVar(&c.ShowAll, "show-all", false, "print all points when diff exists.")
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
	var srcData, destData *FileData
	var srcPtsList, destPtsList [][]Point
	var eg errgroup.Group
	eg.Go(func() error {
		var err error
		if c.SrcURL != "" {
			srcData, srcPtsList, err = readWhisperFileRemote(c.SrcURL, c.Src, c.Now, c.From, c.Until, c.RetID)
			if err != nil {
				return err
			}
		} else {
			srcData, srcPtsList, err = readWhisperFile(c.Src, c.Now, c.From, c.Until, c.RetID)
			if err != nil {
				return err
			}
		}
		return nil
	})
	eg.Go(func() error {
		var err error
		destData, destPtsList, err = readWhisperFile(c.Dest, c.Now, c.From, c.Until, c.RetID)
		if err != nil {
			return err
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return err
	}

	if !Retentions(srcData.Retentions).Equal(destData.Retentions) {
		return errors.New("retentions unmatch between src and dest whisper files")
	}

	srcPlDif, destPlDif := PointsList(srcPtsList).Diff(destPtsList)
	if PointsList(srcPlDif).AllEmpty() && PointsList(destPlDif).AllEmpty() {
		return nil
	}

	err := printDiff(c.TextOut, srcData, destData, srcPtsList, destPtsList, srcPlDif, destPlDif, c.IgnoreSrcEmpty, c.IgnoreDestEmpty, c.ShowAll)
	if err != nil {
		return err
	}

	return ErrDiffFound
}

func printDiff(textOut string, srcData, destData *FileData, srcPtsList, destPtsList, srcPlDif, destPlDif [][]Point, ignoreSrcEmpty, ignoreDestEmpty, showAll bool) error {
	if textOut == "" {
		return nil
	}

	if textOut == "-" {
		return printDiffTo(os.Stdout, srcData, destData, srcPtsList, destPtsList, srcPlDif, destPlDif, ignoreSrcEmpty, ignoreDestEmpty, showAll)
	}

	file, err := os.Create(textOut)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	err = printDiffTo(w, srcData, destData, srcPtsList, destPtsList, srcPlDif, destPlDif, ignoreSrcEmpty, ignoreDestEmpty, showAll)
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

func printDiffTo(w io.Writer, srcData, destData *FileData, srcPtsList, destPtsList, srcPlDif, destPlDif [][]Point, ignoreSrcEmpty, ignoreDestEmpty, showAll bool) error {
	if showAll {
		for retID := range srcData.Retentions {
			srcPts := srcPtsList[retID]
			destPts := destPtsList[retID]
			for i, srcPt := range srcPts {
				destPt := destPts[i]
				var diff int
				if !srcPt.Equal(destPt) &&
					!(srcPt.Time == destPt.Time && ((ignoreSrcEmpty && srcPt.Value.IsNaN()) || (ignoreDestEmpty && destPt.Value.IsNaN()))) {
					diff = 1
				}
				fmt.Fprintf(w, "retID:%d\tt:%s\tsrcVal:%s\tdestVal:%s\tdestMinusSrc:%s\tdiff:%d\n",
					retID, srcPt.Time, srcPt.Value, destPt.Value, destPt.Value.Diff(srcPt.Value), diff)
			}
		}
	}

	for retID := range srcData.Retentions {
		srcPtsDif := srcPlDif[retID]
		destPtsDif := destPlDif[retID]
		for i, srcPt := range srcPtsDif {
			destPt := destPtsDif[i]
			if (ignoreSrcEmpty && srcPt.Value.IsNaN()) || (ignoreDestEmpty && destPt.Value.IsNaN()) {
				continue
			}
			fmt.Fprintf(w, "retID:%d\tt:%s\t\tsrcVal:%s\tdestVal:%s\tdestMinusSrc:%s\n",
				retID, srcPt.Time, srcPt.Value, destPt.Value, destPt.Value.Diff(srcPt.Value))

		}
	}
	return nil
}

func readWhisperFileRemote(srcURL, filename string, now, from, until Timestamp, retID int) (*FileData, [][]Point, error) {
	reqURL := fmt.Sprintf("%s/view?now=%s&file=%s",
		srcURL, url.QueryEscape(now.String()), url.QueryEscape(filename))
	d, err := getFileDataFromRemoteHelper(reqURL)
	if err != nil {
		return nil, nil, err
	}

	pointsList, err := fetchPointsList(d, now, from, until, retID)
	if err != nil {
		return nil, nil, err
	}
	return d, pointsList, nil
}

func getFileDataFromRemoteHelper(reqURL string) (*FileData, error) {
	resp, err := http.Get(reqURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	d, err := NewFileDataRead(data)
	if err != nil {
		return nil, err
	}
	return d, nil
}
