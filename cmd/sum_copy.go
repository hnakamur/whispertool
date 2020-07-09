package cmd

import (
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"time"

	"github.com/hnakamur/whispertool"
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
	ArchiveID   int
	TextOut     string
}

func (c *SumCopyCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcBase, "src-base", "", "src base directory or web app URL of \"whispertool server\"")
	fs.StringVar(&c.ItemPattern, "item", "", "item directory glob pattern relative to src base")
	fs.StringVar(&c.SrcPattern, "src", "", "whisper file glob pattern relative to item directory (ex. *.wsp).")
	fs.StringVar(&c.DestBase, "dest-base", "", "dest base directory")
	fs.StringVar(&c.DestRelPath, "dest", "", "dest whisper relative filename to item directory (ex. dest.wsp).")
	fs.IntVar(&c.ArchiveID, "archive", ArchiveIDAll, "archive ID (-1 is all).")
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
	if isBaseURL(c.DestBase) {
		return errors.New("dest-base must be local directory")
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

	sumHeader, sumTsList, err := sumWhisperFile(c.SrcBase, item, c.SrcPattern, c.ArchiveID, c.From, c.Until, c.Now)
	if err != nil {
		return err

	}
	destFullPath := filepath.Join(c.DestBase, item, c.DestRelPath)
	destDB, err := openOrCreateCopyDestFile(destFullPath, sumHeader)
	if err != nil {
		return err

	}
	defer destDB.Close()

	if !sumHeader.ArchiveInfoList().Equal(destDB.Header().ArchiveInfoList()) {
		return errors.New("retentions unmatch between src and dest whisper files")
	}

	sumPtsList := sumTsList.PointsList()
	if err := updateFileDataWithPointsList(destDB, sumPtsList, c.Now); err != nil {
		return err
	}

	if err := printFileData(c.TextOut, sumHeader, sumPtsList, true); err != nil {
		return err
	}

	if err := destDB.Sync(); err != nil {
		return err
	}
	return nil
}
