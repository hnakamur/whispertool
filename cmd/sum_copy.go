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
	SrcURL      string
	SrcBase     string
	DestBase    string
	ItemPattern string
	SrcPattern  string
	Dest        string
	From        whispertool.Timestamp
	Until       whispertool.Timestamp
	Now         whispertool.Timestamp
	RetID       int
	TextOut     string
}

func (c *SumCopyCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcURL, "src-url", "", "web app URL for src")
	fs.StringVar(&c.SrcBase, "src-base", "", "src base directory")
	fs.StringVar(&c.ItemPattern, "item", "", "glob pattern of whisper directory")
	fs.StringVar(&c.DestBase, "dest-base", "", "dest base directory")
	fs.StringVar(&c.SrcPattern, "src", "", "glob pattern of source whisper files (ex. src/*.wsp).")
	fs.StringVar(&c.Dest, "dest", "", "dest whisper filename (ex. dest.wsp).")
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
	if c.SrcBase == "" && c.SrcURL == "" {
		return newRequiredOptionError(fs, "src-base or src-url")
	}
	if c.DestBase == "" {
		return newRequiredOptionError(fs, "dest-base")
	}
	if c.SrcPattern == "" {
		return newRequiredOptionError(fs, "src")
	}
	if c.Dest == "" {
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

	itemDirnames, err := filepath.Glob(filepath.Join(c.DestBase, c.ItemPattern))
	if err != nil {
		return err
	}
	if len(itemDirnames) == 0 {
		return fmt.Errorf("itemPattern match no directries, itemPattern=%s", c.ItemPattern)
	}
	totalItemCount = len(itemDirnames)

	for _, itemDirname := range itemDirnames {
		itemRelDir, err := filepath.Rel(c.DestBase, itemDirname)
		if err != nil {
			return err
		}
		fmt.Printf("item:%s\n", itemRelDir)
		err = c.sumCopyItem(itemRelDir)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *SumCopyCommand) sumCopyItem(itemRelDir string) error {
	srcFullPattern := filepath.Join(c.SrcBase, itemRelDir, c.SrcPattern)
	var srcFilenames []string
	if c.SrcURL == "" {
		var err error
		srcFilenames, err = filepath.Glob(srcFullPattern)
		if err != nil {
			return err
		}
		if len(srcFilenames) == 0 {
			return fmt.Errorf("no file matched for -src=%s", c.SrcPattern)
		}
	}
	destFull := filepath.Join(c.DestBase, itemRelDir, c.Dest)

	var destDB *whispertool.Whisper
	var sumData *whispertool.FileData
	var sumPtsList [][]whispertool.Point
	var g errgroup.Group
	g.Go(func() error {
		var err error
		if c.SrcURL != "" {
			sumData, sumPtsList, err = sumWhisperFileRemote(c.SrcURL, srcFullPattern, c.RetID, c.From, c.Until, c.Now)
			if err != nil {
				return err
			}
		} else {
			sumData, sumPtsList, err = sumWhisperFile(srcFilenames, c.RetID, c.From, c.Until, c.Now)
			if err != nil {
				return err
			}
		}
		return nil
	})
	g.Go(func() error {
		var err error
		destDB, err = whispertool.OpenForWrite(destFull)
		if err != nil {
			return err
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	defer destDB.Close()

	destData := destDB.FileData()
	if !whispertool.Retentions(sumData.Retentions()).Equal(destData.Retentions()) {
		return errors.New("retentions unmatch between src and dest whisper files")
	}

	if err := updateFileDataWithPointsList(destData, sumPtsList, c.Now); err != nil {
		return err
	}

	if err := printFileData(c.TextOut, sumData, sumPtsList, true); err != nil {
		return err
	}

	if err := destDB.Sync(); err != nil {
		return err
	}
	return nil
}
