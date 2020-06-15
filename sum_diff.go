package whispertool

import (
	"fmt"
	"log"
	"path/filepath"
)

func SumDiff(itemPattern, srcPattern, dest string) error {
	log.Printf("SumDiff start itemPattern=%s, srcPattern=%s, dest=%s", itemPattern, srcPattern, dest)
	itemDirnames, err := filepath.Glob(itemPattern)
	if err != nil {
		return err
	}
	if len(itemDirnames) == 0 {
		return fmt.Errorf("itemPattern match no directries, itemPattern=%s", itemPattern)
	}

	for _, itemDirname := range itemDirnames {
		if err := sumDiffItem(itemDirname, srcPattern, dest); err != nil {
			return err
		}
	}
	return nil
}

func sumDiffItem(item, srcPattern, dest string) error {
	return nil
}
