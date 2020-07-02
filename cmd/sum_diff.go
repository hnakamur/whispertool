package cmd

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hnakamur/whispertool"
	"golang.org/x/sync/errgroup"
)

type SumDiffCommand struct {
	SrcBase     string
	ItemPattern string
	SrcPattern  string
	DestBase    string
	DestRelPath string
	From        whispertool.Timestamp
	Until       whispertool.Timestamp
	Now         whispertool.Timestamp
	RetID       int
	TextOut     string
	SumTextOut  string
	DestTextOut string

	Interval       time.Duration
	IntervalOffset time.Duration
	UntilOffset    time.Duration
}

func (c *SumDiffCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcBase, "src-base", "", "src base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.ItemPattern, "item", "", "item directory glob pattern relative to src base")
	fs.StringVar(&c.SrcPattern, "src", "", "whisper file glob pattern relative to item directory (ex. *.wsp).")
	fs.StringVar(&c.DestBase, "dest-base", "", "dest base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.DestRelPath, "dest", "", "dest whisper filename relative to item directory (ex. sum.wsp).")
	fs.IntVar(&c.RetID, "ret", RetIDAll, "retention ID to diff (-1 is all).")
	fs.StringVar(&c.TextOut, "text-out", "", "text output of diff. empty means no output, - means stdout, other means output file.")
	fs.StringVar(&c.SumTextOut, "sum-text-out", "", "text output of sum. empty means no output, - means stdout, other means output file.")
	fs.StringVar(&c.DestTextOut, "dest-text-out", "", "text output of destination. empty means no output, - means stdout, other means output file.")

	c.Now = whispertool.TimestampFromStdTime(time.Now())
	c.Until = c.Now
	fs.Var(&timestampValue{t: &c.Now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.From}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.Until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")

	fs.DurationVar(&c.Interval, "interval", 0, "run interval (0 means oneshot")
	fs.DurationVar(&c.IntervalOffset, "interval-offset", 7*time.Second, "run interval offset")
	fs.DurationVar(&c.UntilOffset, "until-offset", 0, "until offset")
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
	if c.DestBase == "" {
		return newRequiredOptionError(fs, "dest-base")
	}
	if c.DestRelPath == "" {
		return newRequiredOptionError(fs, "dest")
	}
	return nil
}

func (c *SumDiffCommand) Execute() error {
	if c.Interval == 0 {
		err := c.sumDiffOneTime()
		if err != nil {
			return err
		}
		return nil
	}

	for {
		now := time.Now()
		targetTime := now.Truncate(c.Interval).Add(c.Interval).Add(c.IntervalOffset)
		time.Sleep(targetTime.Sub(now))

		c.Now = whispertool.TimestampFromStdTime(time.Now())
		err := c.sumDiffOneTime()
		if err != nil {
			return err
		}
	}
}

func (c *SumDiffCommand) sumDiffOneTime() error {
	t0 := time.Now()
	fmt.Printf("time:%s\tmsg:start\n", formatTime(t0))
	var totalItemCount int
	defer func() {
		t1 := time.Now()
		fmt.Printf("time:%s\tmsg:finish\tduration:%s\ttotalItemCount:%d\n", formatTime(t1), t1.Sub(t0).String(), totalItemCount)
	}()

	items, err := globItems(c.SrcBase, c.ItemPattern)
	if err != nil {
		return err
	}
	totalItemCount = len(items)
	for _, item := range items {
		err = c.sumDiffItem(item)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *SumDiffCommand) sumDiffItem(item string) error {
	fmt.Printf("item:%s\n", item)

	until := c.Until
	if c.UntilOffset != 0 {
		until = c.Now.Add(-whispertool.Duration(c.UntilOffset / time.Second))
	}

	var sumDB, destDB *whispertool.Whisper
	var sumPtsList, destPtsList PointsList
	var g errgroup.Group
	g.Go(func() error {
		var err error
		sumDB, sumPtsList, err = sumWhisperFile(c.SrcBase, item, c.SrcPattern, c.RetID, c.From, until, c.Now)
		return err
	})
	g.Go(func() error {
		var err error
		destRelPath := filepath.Join(itemToRelDir(item), c.DestRelPath)
		destDB, destPtsList, err = readWhisperFile(c.DestBase, destRelPath, c.RetID, c.From, until, c.Now)
		return err
	})
	if err := g.Wait(); err != nil {
		return err
	}

	if !sumDB.Retentions().Equal(destDB.Retentions()) {
		return errors.New("retentions unmatch between src and dest whisper files")
	}

	sumPlDif, destPlDif := sumPtsList.Diff(destPtsList)
	if sumPtsList.AllEmpty() && destPlDif.AllEmpty() {
		return nil
	}

	err := printDiff(c.TextOut, sumDB, destDB, sumPtsList, destPtsList, sumPlDif, destPlDif)
	if err != nil {
		return err
	}
	if err := printPointsListAppend(c.SumTextOut, item, sumDB, sumPtsList); err != nil {
		return err
	}
	if err := printPointsListAppend(c.DestTextOut, item, destDB, destPtsList); err != nil {
		return err
	}
	return nil
}

func formatTime(t time.Time) string {
	return t.Format(whispertool.UTCTimeLayout)
}

func printPointsListAppend(textOut string, itemName string, db *whispertool.Whisper, ptsList PointsList) error {
	if textOut == "" {
		return nil
	}

	if textOut == "-" {
		return printPointsListAppendTo(os.Stdout, itemName, db, ptsList)
	}

	file, err := os.OpenFile(textOut, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	err = printPointsListAppendTo(w, itemName, db, ptsList)
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

func printPointsListAppendTo(w io.Writer, itemName string, db *whispertool.Whisper, ptsList PointsList) error {
	if _, err := fmt.Fprintf(w, "item:%s\n", itemName); err != nil {
		return err
	}
	for retID := range db.Retentions() {
		for _, pts := range ptsList {
			for _, pt := range pts {
				if _, err := fmt.Fprintf(w, "retID:%d\tt:%s\tvalue:%s\n", retID, pt.Time, pt.Value); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func itemToRelDir(item string) string {
	return strings.ReplaceAll(item, ".", string(filepath.Separator))
}

func relDirToItem(relDir string) string {
	return strings.ReplaceAll(relDir, string(filepath.Separator), ".")
}
