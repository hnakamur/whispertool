package cmd

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/hnakamur/whispertool"
)

const ArchiveIDAll = -1

var debug = os.Getenv("DEBUG") != ""

type ViewCommand struct {
	SrcBase    string
	SrcRelPath string
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
	fs.StringVar(&c.SrcBase, "src-base", "", "src base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.SrcRelPath, "src", "", "whisper file relative path to src base")
	fs.Var(&timestampValue{t: &c.Now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.From}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.Until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")
	fs.IntVar(&c.RetID, "ret", ArchiveIDAll, "retention ID to diff (-1 is all)")
	fs.StringVar(&c.TextOut, "text-out", "-", "text output of copying data. empty means no output, - means stdout, other means output file.")
	fs.BoolVar(&c.ShowHeader, "header", true, "whether or not to show header (metadata and reteions)")
	fs.Parse(args)

	if c.From > c.Until {
		return errFromIsAfterUntil
	}

	return nil
}

func (c *ViewCommand) Execute() error {
	d, tsList, err := readWhisperFile(c.SrcBase, c.SrcRelPath, c.RetID, c.From, c.Until, c.Now)
	if err != nil {
		return err
	}

	if err := printFileData(c.TextOut, d, tsList.PointsList(), c.ShowHeader); err != nil {
		return err
	}
	return nil
}

func readWhisperFile(baseDirOrURL, fileRelPath string, archiveID int, from, until, now whispertool.Timestamp) (*whispertool.Header, TimeSeriesList, error) {
	if isBaseURL(baseDirOrURL) {
		return readWhisperFileRemote(baseDirOrURL, fileRelPath, archiveID, from, until, now)
	}

	fileFullPath := filepath.Join(baseDirOrURL, fileRelPath)
	return readWhisperFileLocal(fileFullPath, archiveID, from, until, now)
}

func readWhisperFileRemote(srcURL, fileRelPath string, archiveID int, from, until, now whispertool.Timestamp) (*whispertool.Header, TimeSeriesList, error) {
	reqURL := fmt.Sprintf("%s/view?file=%s&retention=%d&from=%s&until=%s&now=%s",
		srcURL,
		url.QueryEscape(fileRelPath),
		archiveID,
		url.QueryEscape(from.String()),
		url.QueryEscape(until.String()),
		url.QueryEscape(now.String()))
	h, tsList, err := getFileDataFromRemote(reqURL)
	if err != nil {
		return nil, nil, err
	}

	return h, tsList, nil
}

func getFileDataFromRemote(reqURL string) (*whispertool.Header, TimeSeriesList, error) {
	resp, err := http.Get(reqURL)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	h := &whispertool.Header{}
	if data, err = h.TakeFrom(data); err != nil {
		return nil, nil, err
	}

	tsList := make(TimeSeriesList, len(h.ArchiveInfoList()))
	for i := range h.ArchiveInfoList() {
		if data, err = tsList[i].TakeFrom(data); err != nil {
			return nil, nil, err
		}
	}
	return h, tsList, nil
}

func readWhisperFileLocal(filename string, archiveID int, from, until, now whispertool.Timestamp) (*whispertool.Header, TimeSeriesList, error) {
	db, err := whispertool.Open(filename)
	if err != nil {
		return nil, nil, err
	}
	defer db.Close()

	tsList, err := fetchTimeSeriesList(db, archiveID, from, until, now)
	if err != nil {
		return nil, nil, err
	}
	return db.Header(), tsList, nil
}

func fetchTimeSeriesList(db *whispertool.Whisper, archiveID int, from, until, now whispertool.Timestamp) (TimeSeriesList, error) {
	tsList := make(TimeSeriesList, len(db.ArchiveInfoList()))
	if archiveID == ArchiveIDAll {
		for i := range db.ArchiveInfoList() {
			ts, err := db.FetchFromArchive(i, from, until, now)
			if err != nil {
				return nil, err
			}
			tsList[i] = ts
		}
	} else if archiveID >= 0 && archiveID < len(db.ArchiveInfoList()) {
		ts, err := db.FetchFromArchive(archiveID, from, until, now)
		if err != nil {
			return nil, err
		}
		tsList[archiveID] = ts
	} else {
		return nil, whispertool.ErrArchiveIDOutOfRange
	}
	return tsList, nil
}

func printFileData(textOut string, h *whispertool.Header, ptsList PointsList, showHeader bool) error {
	if textOut == "" {
		return nil
	}

	if textOut == "-" {
		return printHeaderAndPointsList(os.Stdout, h, ptsList, showHeader)
	}

	file, err := os.Create(textOut)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	if err := printHeaderAndPointsList(w, h, ptsList, showHeader); err != nil {
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

func printHeaderAndPointsList(w io.Writer, h *whispertool.Header, ptsList PointsList, showHeader bool) error {
	if showHeader {
		if _, err := fmt.Fprintln(w, h.String()); err != nil {
			return err
		}
	}
	if err := ptsList.Print(w); err != nil {
		return err
	}
	return nil
}
