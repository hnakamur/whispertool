package whispertool

import (
	"time"
)

func SumDiff(srcBase, destBase, itemPattern, srcPattern, dest string, ignoreSrcEmpty, ignoreDestEmpty, showAll bool, interval, intervalOffset, untilOffset time.Duration, retID int) error {
	//	//log.Printf("SumDiff start srcBase=%s, destBase=%s, itemPattern=%s, srcPattern=%s, dest=%s", srcBase, destBase, itemPattern, srcPattern, dest)
	//	if interval == 0 {
	//		err := sumDiffOneTime(srcBase, destBase, itemPattern, srcPattern, dest, ignoreSrcEmpty, ignoreDestEmpty, showAll, untilOffset, retID)
	//		if err != nil {
	//			return err
	//		}
	//		return nil
	//	}
	//
	//	for {
	//		now := time.Now()
	//		targetTime := now.Truncate(interval).Add(interval).Add(intervalOffset)
	//		time.Sleep(targetTime.Sub(now))
	//
	//		err := sumDiffOneTime(srcBase, destBase, itemPattern, srcPattern, dest, ignoreSrcEmpty, ignoreDestEmpty, showAll, untilOffset, retID)
	//		if err != nil {
	//			return err
	//		}
	//	}
	return nil
}

//func sumDiffOneTime(srcBase, destBase, itemPattern, srcPattern, dest string, ignoreSrcEmpty, ignoreDestEmpty, showAll bool, untilOffset time.Duration, retID int) error {
//	t0 := time.Now()
//	fmt.Printf("time:%s\tmsg:start\n", formatTime(t0))
//	var totalItemCount int
//	defer func() {
//		t1 := time.Now()
//		fmt.Printf("time:%s\tmsg:finish\tduration:%s\ttotalItemCount:%d\n", formatTime(t1), t1.Sub(t0).String(), totalItemCount)
//	}()
//
//	itemDirnames, err := filepath.Glob(filepath.Join(srcBase, itemPattern))
//	if err != nil {
//		return err
//	}
//	if len(itemDirnames) == 0 {
//		return fmt.Errorf("itemPattern match no directries, itemPattern=%s", itemPattern)
//	}
//	totalItemCount = len(itemDirnames)
//
//	for _, itemDirname := range itemDirnames {
//		itemRelDir, err := filepath.Rel(srcBase, itemDirname)
//		if err != nil {
//			return err
//		}
//		//fmt.Printf("itemRel:%s\n", itemRelDir)
//		err = sumDiffItem(srcBase, destBase, itemRelDir, srcPattern, dest, ignoreSrcEmpty, ignoreDestEmpty, showAll, untilOffset, retID)
//		if err != nil {
//			return err
//		}
//	}
//	return nil
//}
//
//func sumDiffItem(srcBase, destBase, itemRelDir, srcPattern, dest string, ignoreSrcEmpty, ignoreDestEmpty, showAll bool, untilOffset time.Duration, retID int) error {
//	srcFullPattern := filepath.Join(srcBase, itemRelDir, srcPattern)
//	srcFilenames, err := filepath.Glob(srcFullPattern)
//	if err != nil {
//		return err
//	}
//	if len(srcFilenames) == 0 {
//		return fmt.Errorf("no file matched for -src=%s", srcPattern)
//	}
//	destFull := filepath.Join(destBase, itemRelDir, dest)
//
//	now := time.Now()
//	from := time.Unix(0, 0)
//	until := now.Add(-untilOffset)
//	tsNow := TimestampFromStdTime(now)
//	tsFrom := TimestampFromStdTime(from)
//	tsUntil := TimestampFromStdTime(until)
//
//	var sumData, destData *whisperFileData
//	var g errgroup.Group
//	g.Go(func() error {
//		var err error
//		sumData, err = sumWhisperFile(srcFilenames, tsNow, tsFrom, tsUntil, retID)
//		if err != nil {
//			return err
//		}
//		sumData.filename = srcFilenames[0]
//		return nil
//	})
//	g.Go(func() error {
//		var err error
//		destData, err = readWhisperFile(destFull, tsNow, tsFrom, tsUntil, retID)
//		if err != nil {
//			return err
//		}
//		return nil
//	})
//	if err := g.Wait(); err != nil {
//		return err
//	}
//
//	iss, err := diffIndexesWhisperFileData(sumData, destData, ignoreSrcEmpty, ignoreDestEmpty, showAll, retID)
//	if err != nil {
//		return err
//	}
//
//	if diffIndexesEmpty(iss) {
//		return nil
//	}
//	fmt.Printf("time:%s\tmsg:sum diff found\tsrc:%s\tdest:%s\n",
//		tsNow, srcFullPattern, destFull)
//	writeDiff(iss, sumData, destData, showAll)
//	return nil
//}
