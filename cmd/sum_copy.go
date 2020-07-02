package cmd

import (
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"time"

	"github.com/hnakamur/whispertool"
	"golang.org/x/sync/errgroup"
)

type SumCopyCommand struct {
	SrcBase     string
	DestBase    string
	ItemPattern string
	SrcPattern  string
	DestRelPath string
	From        whispertool.Timestamp
	Until       whispertool.Timestamp
	Now         whispertool.Timestamp
	RetID       int
	TextOut     string
}

func (c *SumCopyCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcBase, "src-base", "", "src base directory or web app URL of \"whispertool server\"")
	fs.StringVar(&c.ItemPattern, "item", "", "item directory glob pattern relative to src base")
	fs.StringVar(&c.SrcPattern, "src", "", "whisper file glob pattern relative to item directory (ex. *.wsp).")
	fs.StringVar(&c.DestBase, "dest-base", "", "dest base directory")
	fs.StringVar(&c.DestRelPath, "dest", "", "dest whisper relative filename to item directory (ex. dest.wsp).")
	fs.IntVar(&c.RetID, "ret", RetIDAll, "retention ID to diff (-1 is all).")
	fs.StringVar(&c.TextOut, "text-out", "", "text output of copying data. empty means no output, - means stdout, other means output file.")

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
	if c.DestBase == "" {
		return newRequiredOptionError(fs, "dest-base")
	}
	if c.DestRelPath == "" {
		return newRequiredOptionError(fs, "dest")
	}
	return nil
}

func (c *SumCopyCommand) Execute() error {
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
	for _, item := range items {
		err = c.sumCopyItem(item)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *SumCopyCommand) sumCopyItem(item string) error {
	fmt.Printf("item:%s\n", item)

	var sumDB, destDB *whispertool.Whisper
	var sumPtsList PointsList
	var g errgroup.Group
	g.Go(func() error {
		var err error
		sumDB, sumPtsList, err = sumWhisperFile(c.SrcBase, item, c.SrcPattern, c.RetID, c.From, c.Until, c.Now)
		return err
	})
	g.Go(func() error {
		destFullPath := filepath.Join(c.DestBase, item, c.DestRelPath)
		var err error
		destDB, err = openOrCreateCopyDestFile(destFullPath, sumDB)
		return err
	})
	if err := g.Wait(); err != nil {
		return err
	}
	defer destDB.Close()

	if !sumDB.Retentions().Equal(destDB.Retentions()) {
		return errors.New("retentions unmatch between src and dest whisper files")
	}

	if err := updateFileDataWithPointsList(destDB, sumPtsList, c.Now); err != nil {
		return err
	}

	if err := printFileData(c.TextOut, sumDB, sumPtsList, true); err != nil {
		return err
	}

	if err := destDB.Sync(); err != nil {
		return err
	}
	return nil
}
