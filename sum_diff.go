package whispertool

import (
	"fmt"
	"log"
	"path/filepath"
	"time"
)

func SumDiff(srcBase, destBase, itemPattern, srcPattern, dest string, ignoreSrcEmpty, showAll bool) error {
	log.Printf("SumDiff start srcBase=%s, destBase=%s, itemPattern=%s, srcPattern=%s, dest=%s", srcBase, destBase, itemPattern, srcPattern, dest)
	itemDirnames, err := filepath.Glob(filepath.Join(srcBase, itemPattern))
	if err != nil {
		return err
	}
	if len(itemDirnames) == 0 {
		return fmt.Errorf("itemPattern match no directries, itemPattern=%s", itemPattern)
	}

	for _, itemDirname := range itemDirnames {
		itemRelDir, err := filepath.Rel(srcBase, itemDirname)
		if err != nil {
			return err
		}
		err = sumDiffItem(srcBase, destBase, itemRelDir, srcPattern, dest, ignoreSrcEmpty, showAll)
		if err != nil {
			return err
		}
	}
	return nil
}

func sumDiffItem(srcBase, destBase, itemRelDir, srcPattern, dest string, ignoreSrcEmpty, showAll bool) error {
	now := time.Now()
	from := time.Unix(0, 0)
	until := now

	srcFilenames, err := filepath.Glob(filepath.Join(srcBase, itemRelDir, srcPattern))
	if err != nil {
		return err
	}
	if len(srcFilenames) == 0 {
		return fmt.Errorf("no file matched for -src=%s", srcPattern)
	}

	sumData, err := sumWhisperFile(srcFilenames, now, from, until)
	if err != nil {
		return err
	}
	sumData.filename = srcFilenames[0]

	destData, err := readWhisperFile(filepath.Join(destBase, itemRelDir, dest), now, from, until, RetIdAll)
	if err != nil {
		return err
	}

	return diffWhisperFileData(sumData, destData, ignoreSrcEmpty, showAll)
}
