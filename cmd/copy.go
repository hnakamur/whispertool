package cmd

import (
	"errors"
	"flag"
	"time"

	"github.com/hnakamur/whispertool"
	"golang.org/x/sync/errgroup"
)

type CopyCommand struct {
	SrcURL  string
	Src     string
	Dest    string
	From    whispertool.Timestamp
	Until   whispertool.Timestamp
	Now     whispertool.Timestamp
	RetID   int
	TextOut string
}

func (c *CopyCommand) Parse(fs *flag.FlagSet, args []string) error {
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

	if c.Src == "" && c.SrcURL == "" {
		return newRequiredOptionError(fs, "src or src-url")
	}
	if c.Dest == "" {
		return newRequiredOptionError(fs, "dest")
	}
	if c.From > c.Until {
		return errFromIsAfterUntil
	}

	return nil
}

func (c *CopyCommand) Execute() error {
	var srcDB, destDB *whispertool.Whisper
	var srcPtsList [][]whispertool.Point
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
		destDB, err = whispertool.Open(c.Dest)
		if err != nil {
			return err
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return err
	}
	defer destDB.Close()

	if !whispertool.Retentions(srcDB.Retentions()).Equal(destDB.Retentions()) {
		return errors.New("retentions unmatch between src and dest whisper files")
	}

	if err := updateFileDataWithPointsList(destDB, srcPtsList, c.Now); err != nil {
		return err
	}

	if err := printFileData(c.TextOut, srcDB, srcPtsList, true); err != nil {
		return err
	}

	if err := destDB.Sync(); err != nil {
		return err
	}
	return nil
}
