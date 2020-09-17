package cmd

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"time"

	"github.com/hnakamur/whispertool"
)

type ViewRawCommand struct {
	SrcBase     string
	SrcRelPath  string
	From        whispertool.Timestamp
	Until       whispertool.Timestamp
	ArchiveID   int
	ShowHeader  bool
	SortsByTime bool
	TextOut     string
}

func (c *ViewRawCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcBase, "src-base", "", "src base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.SrcRelPath, "src", "", "whisper file relative path to src base")
	fs.Var(&timestampValue{t: &c.From}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.Until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")
	fs.IntVar(&c.ArchiveID, "archive", ArchiveIDAll, "archive ID (-1 is all).")
	fs.BoolVar(&c.ShowHeader, "header", true, "whether or not to show header (metadata and reteions)")
	fs.BoolVar(&c.SortsByTime, "sort", false, "whether or not to sorts points by time")
	fs.StringVar(&c.TextOut, "text-out", "-", "text output of copying data. empty means no output, - means stdout, other means output file.")
	fs.Parse(args)

	if c.SrcBase == "" {
		return newRequiredOptionError(fs, "src-base")
	}
	if c.SrcRelPath == "" {
		return newRequiredOptionError(fs, "src")
	}
	if c.From > c.Until {
		return errFromIsAfterUntil
	}

	return nil
}

func (c *ViewRawCommand) Execute() error {
	return withTextOutWriter(c.TextOut, c.execute)
}

func (c *ViewRawCommand) execute(tow io.Writer) (err error) {
	var until whispertool.Timestamp
	if c.Until == 0 {
		until = whispertool.TimestampFromStdTime(time.Now())
	} else {
		until = c.Until
	}

	h, ptsList, err := readWhisperFileRaw(c.SrcBase, c.SrcRelPath, c.ArchiveID)
	if err != nil {
		return err
	}

	ptsList = filterPointsListByTimeRange(h, ptsList, c.From, until)
	if c.SortsByTime {
		sortPointsListByTime(ptsList)
	}

	if err := printFileData(tow, h, ptsList, c.ShowHeader); err != nil {
		return err
	}
	return nil
}

func readWhisperFileRaw(baseDirOrURL, srcRelPath string, archiveID int) (*whispertool.Header, PointsList, error) {
	if isBaseURL(baseDirOrURL) {
		return readWhisperFileRawRemote(baseDirOrURL, srcRelPath, archiveID)
	}
	return readWhisperFileRawLocal(filepath.Join(baseDirOrURL, srcRelPath), archiveID)
}

func readWhisperFileRawRemote(srcURL, srcRelPath string, archiveID int) (*whispertool.Header, PointsList, error) {
	reqURL := fmt.Sprintf("%s/view-raw?file=%s&retention=%d",
		srcURL, url.QueryEscape(srcRelPath), archiveID)
	h, ptsList, err := getRawFileDataFromRemote(reqURL)
	if err != nil {
		return nil, nil, err
	}
	return h, ptsList, nil
}

func getRawFileDataFromRemote(reqURL string) (*whispertool.Header, PointsList, error) {
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

	ptsList := make(PointsList, len(h.ArchiveInfoList()))
	for i := range h.ArchiveInfoList() {
		if data, err = ptsList[i].TakeFrom(data); err != nil {
			return nil, nil, err
		}
	}
	return h, ptsList, nil
}

func readWhisperFileRawLocal(filename string, archiveID int) (*whispertool.Header, PointsList, error) {
	db, err := whispertool.Open(filename)
	if err != nil {
		return nil, nil, err
	}
	defer db.Close()

	ptsList, err := fetchRawPointsLists(db, archiveID)
	if err != nil {
		return nil, nil, err
	}
	return db.Header(), ptsList, nil
}

func fetchRawPointsLists(db *whispertool.Whisper, archiveID int) (PointsList, error) {
	ptsList := make(PointsList, len(db.ArchiveInfoList()))
	var err error
	if archiveID == ArchiveIDAll {
		for i := range db.ArchiveInfoList() {
			ptsList[i], err = db.GetAllRawUnsortedPoints(i)
			if err != nil {
				return nil, err
			}
		}
	} else if archiveID >= 0 && archiveID < len(db.ArchiveInfoList()) {
		ptsList[archiveID], err = db.GetAllRawUnsortedPoints(archiveID)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, whispertool.ErrArchiveIDOutOfRange
	}
	return ptsList, nil
}

func filterPointsListByTimeRange(h *whispertool.Header, pointsList PointsList, from, until whispertool.Timestamp) PointsList {
	pointsList2 := make([]whispertool.Points, len(pointsList))
	for i := range h.ArchiveInfoList() {
		r := &h.ArchiveInfoList()[i]
		pointsList2[i] = filterPointsByTimeRange(r, pointsList[i], from, until)
	}
	return pointsList2
}

func filterPointsByTimeRange(r *whispertool.ArchiveInfo, points []whispertool.Point, from, until whispertool.Timestamp) []whispertool.Point {
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

func sortPointsListByTime(pointsList PointsList) {
	for _, points := range pointsList {
		sort.Stable(points)
	}
}
