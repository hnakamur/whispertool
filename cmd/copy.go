package cmd

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"time"

	"github.com/hnakamur/whispertool"
)

type CopyCommand struct {
	SrcBase     string
	SrcRelPath  string
	DestBase    string
	DestRelPath string
	From        whispertool.Timestamp
	Until       whispertool.Timestamp
	Now         whispertool.Timestamp
	RetID       int
	TextOut     string
}

func (c *CopyCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcBase, "src-base", "", "src base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.SrcRelPath, "src", "", "whisper file relative path to src base")
	fs.StringVar(&c.DestBase, "dest-base", "", "dest base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.DestRelPath, "dest", "", "whisper file relative path to dest base")
	fs.IntVar(&c.RetID, "ret", RetIDAll, "retention ID to diff (-1 is all).")
	fs.StringVar(&c.TextOut, "text-out", "", "text output of copying data. empty means no output, - means stdout, other means output file.")

	c.Now = whispertool.TimestampFromStdTime(time.Now())
	c.Until = c.Now
	fs.Var(&timestampValue{t: &c.Now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.From}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.Until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")
	fs.Parse(args)

	if c.SrcBase == "" {
		return newRequiredOptionError(fs, "src-base")
	}
	if c.SrcRelPath == "" {
		return newRequiredOptionError(fs, "src")
	}
	if c.DestBase == "" {
		return newRequiredOptionError(fs, "dest-base")
	}
	if c.DestRelPath == "" {
		return newRequiredOptionError(fs, "dest")
	}
	if isBaseURL(c.DestBase) {
		return errors.New("not implemented yet for remote destination, currently only local destination is supported")
	}
	if c.From > c.Until {
		return errFromIsAfterUntil
	}

	return nil
}

func (c *CopyCommand) Execute() error {
	srcDB, srcPtsList, err := readWhisperFile(c.SrcBase, c.SrcRelPath, c.RetID, c.From, c.Until, c.Now)
	if err != nil {
		return err
	}

	destFullPath := filepath.Join(c.DestBase, c.DestRelPath)
	destDB, err := openOrCreateCopyDestFile(destFullPath, srcDB)
	if err != nil {
		return err
	}
	defer destDB.Close()

	if !srcDB.Retentions().Equal(destDB.Retentions()) {
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

func openOrCreateCopyDestFile(filename string, srcDB *whispertool.Whisper) (*whispertool.Whisper, error) {
	destDB, err := whispertool.Open(filename, whispertool.WithFlock())
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		destDB, err = whispertool.Create(filename, srcDB.Retentions(),
			srcDB.AggregationMethod(), srcDB.XFilesFactor(), whispertool.WithFlock())
		if err != nil {
			return nil, err
		}
	}
	return destDB, nil
}
