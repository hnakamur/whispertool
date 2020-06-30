package whispertool

import (
	"errors"
	"flag"
	"time"

	"golang.org/x/sync/errgroup"
)

type CopyCommand struct {
	SrcURL  string
	Src     string
	Dest    string
	From    Timestamp
	Until   Timestamp
	Now     Timestamp
	RetID   int
	TextOut string
}

func (c *CopyCommand) Parse(fs *flag.FlagSet, args []string) error {
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
	var destDB *Whisper
	var srcData *FileData
	var srcPtsList [][]Point
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
		destDB, err = OpenForWrite(c.Dest)
		if err != nil {
			return err
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return err
	}
	defer destDB.Close()

	destData := destDB.fileData
	if !Retentions(srcData.retentions).Equal(destData.retentions) {
		return errors.New("retentions unmatch between src and dest whisper files")
	}

	if err := updateFileDataWithPointsList(destData, srcPtsList, c.Now); err != nil {
		return err
	}

	if err := printFileData(c.TextOut, srcData, srcPtsList, true); err != nil {
		return err
	}

	if err := destDB.Sync(); err != nil {
		return err
	}
	return nil
}
