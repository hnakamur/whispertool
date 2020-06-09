package whispertool

import (
	"fmt"
	"math"
	"path/filepath"
	"time"

	whisper "github.com/go-graphite/go-whisper"
	"golang.org/x/sync/errgroup"
)

func Sum(srcPattern, destFilename string) error {
	srcFilenames, err := filepath.Glob(srcPattern)
	if err != nil {
		return err
	}
	if len(srcFilenames) == 0 {
		return fmt.Errorf("no file matched for -src=%s", srcPattern)
	}

	srcDatas := make([]*whisperFileData, len(srcFilenames))
	now := time.Now()
	from := time.Unix(0, 0)
	until := now
	var g errgroup.Group
	for i, srcFilename := range srcFilenames {
		i := i
		srcFilename := srcFilename
		g.Go(func() error {
			d, err := readWhisperFile(srcFilename, now, from, until, RetIdAll)
			if err != nil {
				return err
			}

			srcDatas[i] = d
			return nil
		})
	}
	if err = g.Wait(); err != nil {
		return err
	}

	for i := 1; i < len(srcDatas); i++ {
		if !retentionsEqual(srcDatas[0].retentions, srcDatas[i].retentions) {
			return fmt.Errorf("%s and %s archive confiugrations are unalike. "+
				"Resize the input before diffing", srcFilenames[0], srcFilenames[i])
		}
	}

	destData := sumWhisperFileData(srcDatas)
	return createWhisperFile(destFilename, destData)
}

func sumWhisperFileData(srcDatas []*whisperFileData) *whisperFileData {
	destTss := make([][]*whisper.TimeSeriesPoint, len(srcDatas[0].tss))
	for i := range destTss {
		destTss[i] = sumTimeSeriesPointInFileData(srcDatas, i)
	}

	return &whisperFileData{
		retentions:   srcDatas[0].retentions,
		aggMethod:    srcDatas[0].aggMethod,
		xFilesFactor: srcDatas[0].xFilesFactor,
		tss:          destTss,
	}
}

func sumTimeSeriesPointInFileData(srcDatas []*whisperFileData, retentionId int) []*whisper.TimeSeriesPoint {
	destTs := deepCloneTimeSeriesPoint(srcDatas[0].tss[retentionId])
	for i := 1; i < len(srcDatas); i++ {
		addTimeSeriesPointTo(destTs, srcDatas[i].tss[retentionId])
	}
	return destTs
}

func deepCloneTimeSeriesPoint(pts []*whisper.TimeSeriesPoint) []*whisper.TimeSeriesPoint {
	pts2 := make([]*whisper.TimeSeriesPoint, len(pts))
	for i, pt := range pts {
		pts2[i] = &whisper.TimeSeriesPoint{
			Time:  pt.Time,
			Value: pt.Value,
		}
	}
	return pts2
}

func addTimeSeriesPointTo(dest, src []*whisper.TimeSeriesPoint) {
	for i, pt := range src {
		if math.IsNaN(dest[i].Value) {
			dest[i].Value = pt.Value
		} else if !math.IsNaN(pt.Value) {
			dest[i].Value += pt.Value
		}
	}
}
