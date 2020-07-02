package cmd

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/hnakamur/whispertool"
	"golang.org/x/sync/errgroup"
)

type SumCommand struct {
	SrcBase     string
	ItemPattern string
	SrcPattern  string
	From        whispertool.Timestamp
	Until       whispertool.Timestamp
	Now         whispertool.Timestamp
	RetID       int
	TextOut     string
	ShowHeader  bool
}

func (c *SumCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcBase, "src-base", "", "src base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.ItemPattern, "item", "", "item directory glob pattern relative to src base")
	fs.StringVar(&c.SrcPattern, "src", "", "whisper file glob pattern relative to item directory (ex. *.wsp).")
	fs.IntVar(&c.RetID, "ret", RetIDAll, "retention ID to diff (-1 is all).")
	fs.StringVar(&c.TextOut, "text-out", "-", "text output of copying data. empty means no output, - means stdout, other means output file.")
	fs.BoolVar(&c.ShowHeader, "header", true, "whether or not to show header (metadata and reteions)")

	c.Now = whispertool.TimestampFromStdTime(time.Now())
	c.Until = c.Now
	fs.Var(&timestampValue{t: &c.Now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.From}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.Until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")
	fs.Parse(args)

	if c.ItemPattern == "" {
		return newRequiredOptionError(fs, "item")
	}
	if c.SrcBase == "" {
		return newRequiredOptionError(fs, "src-base")
	}
	if c.SrcPattern == "" {
		return newRequiredOptionError(fs, "src")
	}
	if c.From > c.Until {
		return errFromIsAfterUntil
	}

	return nil
}

func (c *SumCommand) Execute() error {
	items, err := globItems(c.SrcBase, c.ItemPattern)
	if err != nil {
		return err
	}
	for _, item := range items {
		fmt.Printf("item:%s\n", item)
		db, ptsList, err := sumWhisperFile(c.SrcBase, item, c.SrcPattern, c.RetID, c.From, c.Until, c.Now)
		if err != nil {
			return err
		}
		if err := printFileData(c.TextOut, db, ptsList, c.ShowHeader); err != nil {
			return err
		}
	}
	return nil
}

func isBaseURL(baseDirOrURL string) bool {
	return strings.HasPrefix(baseDirOrURL, "http://") || strings.HasPrefix(baseDirOrURL, "https://")
}

func globItems(baseDirOrURL, itemDirPattern string) ([]string, error) {
	if isBaseURL(baseDirOrURL) {
		return globItemsRemote(baseDirOrURL, itemDirPattern)
	}
	return globItemsLocal(baseDirOrURL, itemDirPattern)
}

func globItemsLocal(baseDir, itemDirPattern string) ([]string, error) {
	items, err := filepath.Glob(filepath.Join(baseDir, itemDirPattern))
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("itemPattern match no directries, itemPattern=%s", itemDirPattern)
	}
	for i, itemDirname := range items {
		itemRelDir, err := filepath.Rel(baseDir, itemDirname)
		if err != nil {
			return nil, err
		}
		items[i] = relDirToItem(itemRelDir)
	}
	return items, nil
}

func globItemsRemote(srcURL, itemRelDirPattern string) ([]string, error) {
	reqURL := fmt.Sprintf("%s/items?pattern=%s",
		srcURL,
		url.QueryEscape(itemRelDirPattern))
	resp, err := http.Get(reqURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// We must always read to the end.
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var items []string
	s := bufio.NewScanner(bytes.NewBuffer(data))
	for s.Scan() {
		items = append(items, s.Text())
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func sumWhisperFile(baseDirOrURL, item, srcPattern string, retID int, from, until, now whispertool.Timestamp) (*whispertool.Whisper, PointsList, error) {
	if isBaseURL(baseDirOrURL) {
		return sumWhisperFileRemote(baseDirOrURL, item, srcPattern, retID, from, until, now)
	}
	return sumWhisperFileLocal(baseDirOrURL, item, srcPattern, retID, from, until, now)
}

func sumWhisperFileLocal(baseDir, item, srcPattern string, retID int, from, until, now whispertool.Timestamp) (*whispertool.Whisper, PointsList, error) {
	itemRelDir := itemToRelDir(item)
	srcFullPattern := filepath.Join(baseDir, itemRelDir, srcPattern)
	srcFilenames, err := filepath.Glob(srcFullPattern)
	if err != nil {
		return nil, nil, err
	}
	if len(srcFilenames) == 0 {
		return nil, nil, fmt.Errorf("no file matched for -src=%s", srcPattern)
	}

	srcDBList := make([]*whispertool.Whisper, len(srcFilenames))
	ptsListList := make([]PointsList, len(srcFilenames))
	var g errgroup.Group
	for i, srcFilename := range srcFilenames {
		i := i
		srcFilename := srcFilename
		g.Go(func() error {
			srcRelPath, err := filepath.Rel(baseDir, srcFilename)
			if err != nil {
				return err
			}
			db, ptsList, err := readWhisperFile(baseDir, srcRelPath, retID, from, until, now)
			if err != nil {
				return err
			}

			srcDBList[i] = db
			ptsListList[i] = ptsList
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	for i := 1; i < len(srcDBList); i++ {
		if !srcDBList[0].Retentions().Equal(srcDBList[i].Retentions()) {
			return nil, nil, fmt.Errorf("%s and %s archive confiugrations are unalike. "+
				"Resize the input before summing", srcFilenames[0], srcFilenames[i])
		}
	}

	srcDB0 := srcDBList[0]
	sumDB, err := whispertool.Create("", srcDB0.Retentions(), srcDB0.AggregationMethod(), srcDB0.XFilesFactor(),
		whispertool.WithInMemory())
	if err != nil {
		return nil, nil, err
	}

	sumPtsList := sumPointsLists(ptsListList)

	return sumDB, sumPtsList, nil
}

func sumWhisperFileRemote(srcURL, item, srcPattern string, retID int, from, until, now whispertool.Timestamp) (*whispertool.Whisper, PointsList, error) {
	reqURL := fmt.Sprintf("%s/sum?item=%s&pattern=%s&retention=%d&from=%s&until=%s&now=%s",
		srcURL,
		url.QueryEscape(item),
		url.QueryEscape(srcPattern),
		retID,
		url.QueryEscape(from.String()),
		url.QueryEscape(until.String()),
		url.QueryEscape(now.String()))
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

func sumPointsLists(ptsListList []PointsList) PointsList {
	if len(ptsListList) == 0 {
		return nil
	}
	retentionCount := len(ptsListList[0])
	sumPtsList := make([]whispertool.Points, retentionCount)
	for retID := range sumPtsList {
		sumPtsList[retID] = sumPointsListsForRetention(ptsListList, retID)
	}
	return sumPtsList
}

func sumPointsListsForRetention(ptsListList []PointsList, retID int) []whispertool.Point {
	if len(ptsListList) == 0 {
		return nil
	}
	ptsCount := len(ptsListList[0][retID])
	sumPoints := make([]whispertool.Point, ptsCount)
	for i := range ptsListList {
		for j := range sumPoints {
			if i == 0 {
				sumPoints[j] = ptsListList[i][retID][j]
			} else {
				sumPoints[j].Value = sumPoints[j].Value.Add(ptsListList[i][retID][j].Value)
			}
		}
	}
	return sumPoints
}
