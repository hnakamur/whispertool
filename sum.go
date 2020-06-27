package whispertool

func RunSum(srcPattern, destFilename, textOut string, retId int) error {
	//	if destFilename != "" {
	//		return errors.New("writing sum to whisperfile is not implemented yet")
	//	}
	//
	//	srcFilenames, err := filepath.Glob(srcPattern)
	//	if err != nil {
	//		return err
	//	}
	//	if len(srcFilenames) == 0 {
	//		return fmt.Errorf("no file matched for -src=%s", srcPattern)
	//	}
	//
	//	now := time.Now()
	//	from := time.Unix(0, 0)
	//	until := now
	//	tsNow := TimestampFromStdTime(now)
	//	tsFrom := TimestampFromStdTime(from)
	//	tsUntil := TimestampFromStdTime(until)
	//	sumData, err := sumWhisperFile(srcFilenames, tsNow, tsFrom, tsUntil, retId)
	//	if err != nil {
	//		return err
	//	}
	//
	//	showHeader := true
	//	if err := writeWhisperFileData(textOut, sumData, showHeader); err != nil {
	//		return err
	//	}
	return nil
}

//func sumWhisperFile(srcFilenames []string, now, from, until Timestamp, retId int) (*whisperFileData, error) {
//retry:
//	srcDatas := make([]*whisperFileData, len(srcFilenames))
//	var g errgroup.Group
//	for i, srcFilename := range srcFilenames {
//		i := i
//		srcFilename := srcFilename
//		g.Go(func() error {
//			d, err := readWhisperFile(srcFilename, now, from, until, retId)
//			if err != nil {
//				return err
//			}
//
//			srcDatas[i] = d
//			return nil
//		})
//	}
//	if err := g.Wait(); err != nil {
//		return nil, err
//	}
//
//	for i := 1; i < len(srcDatas); i++ {
//		if !retentionsEqual(srcDatas[0].retentions, srcDatas[i].retentions) {
//			return nil, fmt.Errorf("%s and %s archive confiugrations are unalike. "+
//				"Resize the input before summing", srcFilenames[0], srcFilenames[i])
//		}
//	}
//
//	for i := 1; i < len(srcDatas); i++ {
//		if err := timeDiffMultiArchivePoints(srcDatas[0].pointsList, srcDatas[i].pointsList); err != nil {
//			log.Printf("sum failed since %s and %s archive time values are unalike: %s",
//				srcFilenames[0], srcFilenames[i], err.Error())
//			goto retry
//		}
//	}
//
//	return sumWhisperFileData(srcDatas), nil
//}
//
//func sumWhisperFileData(srcDatas []*whisperFileData) *whisperFileData {
//	destTss := make([][]Point, len(srcDatas[0].pointsList))
//	for i := range destTss {
//		destTss[i] = sumTimeSeriesPointInFileData(srcDatas, i)
//	}
//
//	return &whisperFileData{
//		retentions:        srcDatas[0].retentions,
//		aggregationMethod: srcDatas[0].aggregationMethod,
//		xFilesFactor:      srcDatas[0].xFilesFactor,
//		pointsList:        destTss,
//	}
//}
//
//func sumTimeSeriesPointInFileData(srcDatas []*whisperFileData, retentionId int) []Point {
//	destTs := deepClonePoints(srcDatas[0].pointsList[retentionId])
//	for i := 1; i < len(srcDatas); i++ {
//		addTimeSeriesPointTo(destTs, srcDatas[i].pointsList[retentionId])
//	}
//	return destTs
//}
//
//func deepClonePoints(pts []Point) []Point {
//	pts2 := make([]Point, len(pts))
//	for i, pt := range pts {
//		pts2[i] = Point{
//			Time:  pt.Time,
//			Value: pt.Value,
//		}
//	}
//	return pts2
//}
//
//func addTimeSeriesPointTo(dest, src []Point) {
//	for i, pt := range src {
//		if dest[i].Value.IsNaN() {
//			dest[i].Value = pt.Value
//		} else if !pt.Value.IsNaN() {
//			dest[i].Value += pt.Value
//		}
//	}
//}
