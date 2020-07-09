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
	ArchiveID   int
	TextOut     string
}

func (c *CopyCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcBase, "src-base", "", "src base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.SrcRelPath, "src", "", "whisper file relative path to src base")
	fs.StringVar(&c.DestBase, "dest-base", "", "dest base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.DestRelPath, "dest", "", "whisper file relative path to dest base")
	fs.IntVar(&c.ArchiveID, "archive", ArchiveIDAll, "archive ID (-1 is all).")
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
		return errors.New("dest-base must be local directory")
	}
	if c.From > c.Until {
		return errFromIsAfterUntil
	}

	return nil
}

func (c *CopyCommand) Execute() error {
	srcHeader, srcTsList, err := readWhisperFile(c.SrcBase, c.SrcRelPath, c.ArchiveID, c.From, c.Until, c.Now)
	if err != nil {
		return err
	}

	destFullPath := filepath.Join(c.DestBase, c.DestRelPath)
	destDB, err := openOrCreateCopyDestFile(destFullPath, srcHeader)
	if err != nil {
		return err
	}
	defer destDB.Close()

	if !srcHeader.ArchiveInfoList().Equal(destDB.ArchiveInfoList()) {
		return errors.New("archive info list unmatch between src and dest whisper files")
	}

	if err := updateFileDataWithPointsList(destDB, srcTsList.PointsList(), c.Now); err != nil {
		return err
	}

	if err := printFileData(c.TextOut, srcHeader, srcTsList.PointsList(), true); err != nil {
		return err
	}

	if err := destDB.Sync(); err != nil {
		return err
	}
	return nil
}

func openOrCreateCopyDestFile(filename string, srcHeader *whispertool.Header) (*whispertool.Whisper, error) {
	destDB, err := whispertool.Open(filename)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		destDB, err = whispertool.Create(filename, srcHeader.ArchiveInfoList(),
			srcHeader.AggregationMethod(), srcHeader.XFilesFactor())
		if err != nil {
			return nil, err
		}
	}
	return destDB, nil
}
