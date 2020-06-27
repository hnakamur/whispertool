package whispertool

import (
	"errors"
	"os"
	"time"
)

const RetIdAll = -1

var debug = os.Getenv("DEBUG") != ""
var errRetIdOufOfRange = errors.New("retention id is out of range")

func View(filename string, raw bool, now, from, until time.Time, retId int, showHeader bool) error {
	//if raw {
	_, err := viewRaw(filename, from, until, retId, showHeader)
	return err
	//}
	//tsNow := TimestampFromStdTime(now)
	//tsFrom := TimestampFromStdTime(from)
	//tsUntil := TimestampFromStdTime(until)
	//return view(filename, tsNow, tsFrom, tsUntil, retId, showHeader)
}

//type whisperFileData struct {
//	filename          string
//	aggregationMethod AggregationMethod
//	xFilesFactor      float32
//	retentions        []Retention
//	pointsList        [][]Point
//}
//
//func readWhisperFile(filename string, now, from, until Timestamp, retId int) (*whisperFileData, error) {
//	p := NewBufferPool(os.Getpagesize())
//	db, err := Open(filename, p)
//	if err != nil {
//		return nil, err
//	}
//	defer db.Close()
//
//	return readWhisperDB(db, now, from, until, retId, filename)
//}
//
//func readWhisperDB(db *Whisper, now, from, until Timestamp, retId int, filename string) (*whisperFileData, error) {
//	if debug {
//		log.Printf("readWhisperDB start, filename=%s, from=%s, until=%s, retId=%d",
//			filename, from, until, retId)
//	}
//
//	retentions := db.Retentions
//	pointsList := make([][]Point, len(retentions))
//	if retId == RetIdAll {
//		var g errgroup.Group
//		for i := range retentions {
//			i := i
//			g.Go(func() error {
//				ts, err := readWhisperSingleArchive(db, now, from, until, i, filename)
//				if err != nil {
//					return err
//				}
//				log.Printf("reading one of all archives, retID=%d, len(ts)=%d", i, len(ts))
//				pointsList[i] = ts
//				return nil
//			})
//			if err := g.Wait(); err != nil {
//				return nil, err
//			}
//		}
//	} else if 0 <= retId && retId < len(retentions) {
//		ts, err := readWhisperSingleArchive(db, now, from, until, retId, filename)
//		if err != nil {
//			return nil, err
//		}
//		pointsList[retId] = ts
//	} else {
//		return nil, errRetIdOufOfRange
//	}
//
//	return &whisperFileData{
//		filename:          filename,
//		aggregationMethod: db.Meta.AggregationMethod,
//		xFilesFactor:      db.Meta.XFilesFactor,
//		retentions:        retentions,
//		pointsList:        pointsList,
//	}, nil
//}
//
//func readWhisperSingleArchive(db *Whisper, now, from, until Timestamp, retId int, filename string) ([]Point, error) {
//	if until > now {
//		until = now
//	}
//
//	fetchFrom := from
//	fetchUntil := until
//
//	retentions := db.Retentions
//	r := retentions[retId]
//	minFrom := now.Add(-r.MaxRetention())
//	step := r.SecondsPerPoint
//
//	//if debug {
//	log.Printf("readWhisperSingleArchive retId=%d, from=%s, until=%s, minFrom=%s",
//		retId, from, until, minFrom)
//	//}
//	//if fetchFrom < minFrom {
//	//	if debug {
//	//		log.Printf("adjust fetchFrom to minFrom")
//	//	}
//	//	fetchFrom = minFrom
//	//} else {
//	//	// NOTE: We need to adjust from and until by subtracting step
//	//	// since step is added to from and until in
//	//	// go-whisper archiveInfo.Interval method.
//	//	// https://github.com/go-graphite/go-whisper/blob/e5e7d31ca75557a461f9883667028ddc44713481/whisper.go#L1411
//	//	//
//	//	// I suppose archiveInfo.Interval follows
//	//	// __archive_fetch in original graphite-project/whisper.
//	//	// https://github.com/graphite-project/whisper/blob/master/whisper.py#L970-L972
//	//	// I asked why step is added at
//	//	// https://answers.launchpad.net/graphite/+question/294817
//	//	// but no answer from the person who only knows
//	//	// the original reason.
//	//	fetchFrom = fetchFrom.Add(-step)
//	//	fetchUntil = fetchUntil.Add(-step)
//	//	if debug {
//	//		log.Printf("adjust time range by subtracting step, fetchFrom=%s",
//	//			fetchFrom)
//	//	}
//	//}
//
//	if fetchUntil < fetchFrom {
//		return nil, nil
//	}
//
//	exptectedPtsLen := fetchUntil.Sub(fetchFrom) / step
//	if exptectedPtsLen == 0 {
//		exptectedPtsLen = 1
//	}
//
//	//if debug {
//	log.Printf("calling db.Fetch with retId=%d, fetchFrom=%s, fetchUntil=%s",
//		retId, fetchFrom, fetchUntil)
//	//}
//	pts, err := db.FetchFromArchive(retId, fetchFrom, fetchUntil, now)
//	if err != nil {
//		return nil, err
//	}
//	if debug {
//		for i, pt := range pts {
//			log.Printf("i=%d, pt.Time=%s, pt.Value=%s", i, pt.Time, pt.Value)
//		}
//	}
//	return pts, nil
//}
//
//func view(filename string, now, from, until Timestamp, retId int, showHeader bool) error {
//	d, err := readWhisperFile(filename, now, from, until, retId)
//	if err != nil {
//		return err
//	}
//
//	return d.WriteTo(os.Stdout, showHeader)
//}
//
//func writeWhisperFileData(textOut string, d *whisperFileData, showHeader bool) error {
//	if textOut == "" {
//		return nil
//	}
//
//	if textOut == "-" {
//		return d.WriteTo(os.Stdout, showHeader)
//	}
//
//	file, err := os.Create(textOut)
//	if err != nil {
//		return err
//	}
//	defer file.Close()
//
//	w := bufio.NewWriter(file)
//	if err = d.WriteTo(w, showHeader); err != nil {
//		return err
//	}
//	if err = w.Flush(); err != nil {
//		return err
//	}
//	if err = file.Sync(); err != nil {
//		return err
//	}
//	return nil
//}
//
//func (d *whisperFileData) WriteTo(w io.Writer, showHeader bool) error {
//	if showHeader {
//		_, err := fmt.Fprintf(w, "aggMethod:%s\txFilesFactor:%s\n",
//			d.aggregationMethod,
//			strconv.FormatFloat(float64(d.xFilesFactor), 'f', -1, 32))
//		if err != nil {
//			return err
//		}
//
//		for i, r := range d.retentions {
//			_, err := fmt.Fprintf(w, "retentionDef:%d\tstep:%s\tnumberOfPoints:%d\n",
//				i, r.SecondsPerPoint, r.NumberOfPoints)
//			if err != nil {
//				return err
//			}
//		}
//	}
//	return writeTimeSeriesForArchives(w, d.pointsList)
//}
//
//func writeTimeSeriesForArchives(w io.Writer, pointsList [][]Point) error {
//	for i, points := range pointsList {
//		for _, p := range points {
//			_, err := fmt.Fprintf(w, "retId:%d\tt:%s\tval:%s\n", i, p.Time, p.Value)
//			if err != nil {
//				return err
//			}
//		}
//	}
//	return nil
//}
