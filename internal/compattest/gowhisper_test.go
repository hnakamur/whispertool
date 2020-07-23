package compattest

import (
	"errors"

	"github.com/go-graphite/go-whisper"
	"github.com/hnakamur/whispertool"
	"github.com/hnakamur/whispertool/cmd"
	"golang.org/x/sync/errgroup"
)

type GoWhisperDB struct {
	db *whisper.Whisper
}

func CreateGoWhisperDB(filename string, retentionDefs string, aggregationMethod string, xFilesFactor float32) (*GoWhisperDB, error) {
	retentions, err := whisper.ParseRetentionDefs(retentionDefs)
	if err != nil {
		return nil, err
	}

	toolAggMethod, err := whispertool.AggregationMethodString(aggregationMethod)
	if err != nil {
		return nil, err
	}

	var aggMethod whisper.AggregationMethod
	switch toolAggMethod {
	case whispertool.Average:
		aggMethod = whisper.Average
	case whispertool.Sum:
		aggMethod = whisper.Sum
	case whispertool.Last:
		aggMethod = whisper.Last
	case whispertool.Max:
		aggMethod = whisper.Max
	case whispertool.Min:
		aggMethod = whisper.Min
	case whispertool.First:
		aggMethod = whisper.First
	default:
		return nil, errors.New("invalid aggregation method")
	}

	opts := &whisper.Options{InMemory: true}
	db, err := whisper.CreateWithOptions(filename, retentions, aggMethod, xFilesFactor, opts)
	if err != nil {
		return nil, err
	}
	return &GoWhisperDB{db: db}, nil
}

func OpenGoWhisperDB(filename string) (*GoWhisperDB, error) {
	db, err := whisper.Open(filename)
	if err != nil {
		return nil, err
	}
	return &GoWhisperDB{db: db}, nil
}

func (db *GoWhisperDB) ArciveInfoList() whispertool.ArchiveInfoList {
	return convertGoWhisperRetentions(db.db.Retentions())
}

func (db *GoWhisperDB) Update(t whispertool.Timestamp, value whispertool.Value) error {
	return db.db.Update(float64(value), int(t))
}

func (db *GoWhisperDB) UpdatePointsForArchive(points []whispertool.Point, archiveID int) error {
	return db.db.UpdateManyForArchive(
		convertToGoWhisperTimeSeriesPointPointers(points),
		db.db.Retentions()[archiveID].MaxRetention())
}

func (db *GoWhisperDB) Sync() error {
	return nil
}

func (db *GoWhisperDB) Close() error {
	return db.db.Close()
}

func (db *GoWhisperDB) Fetch(from, until whispertool.Timestamp) (*whispertool.TimeSeries, error) {
	ts, err := db.db.Fetch(int(from), int(until))
	if err != nil {
		return nil, err
	}
	return convertGoWhisperTimeSeries(ts), nil
}

func convertToGoWhisperTimeSeriesPointPointers(pts []whispertool.Point) []*whisper.TimeSeriesPoint {
	if pts == nil {
		return nil
	}
	points := make([]*whisper.TimeSeriesPoint, len(pts))
	for i, p := range pts {
		points[i] = &whisper.TimeSeriesPoint{
			Time:  int(p.Time),
			Value: float64(p.Value),
		}
	}
	return points
}

func convertGoWhisperRetentions(retentions []whisper.Retention) whispertool.ArchiveInfoList {
	archiveInfoList := make(whispertool.ArchiveInfoList, len(retentions))
	for i, r := range retentions {
		archiveInfoList[i] = whispertool.NewArchiveInfo(
			whispertool.Duration(r.SecondsPerPoint()),
			uint32(r.NumberOfPoints()))
	}
	return archiveInfoList
}

func convertGoWhisperTimeSeries(ts *whisper.TimeSeries) *whispertool.TimeSeries {
	if ts == nil {
		return nil
	}
	from := whispertool.Timestamp(ts.FromTime())
	until := whispertool.Timestamp(ts.UntilTime())
	step := whispertool.Duration(ts.Step())
	values := convertValues(ts.Values())
	return whispertool.NewTimeSeries(from, until, step, values)
}

func convertValues(values []float64) []whispertool.Value {
	vv := make([]whispertool.Value, len(values))
	for i, v := range values {
		vv[i] = whispertool.Value(v)
	}
	return vv
}

func (db *GoWhisperDB) fetchAllArchives() (cmd.TimeSeriesList, error) {
	tl := make(cmd.TimeSeriesList, len(db.ArciveInfoList()))
	now := whispertool.TimestampFromStdTime(whisper.Now())
	var eg errgroup.Group
	for archiveID, archiveInfo := range db.ArciveInfoList() {
		archiveID := archiveID
		archiveInfo := archiveInfo
		eg.Go(func() error {
			until := now
			from := now.Add(-archiveInfo.MaxRetention())
			ts, err := db.Fetch(from, until)
			if err != nil {
				return err
			}
			tl[archiveID] = ts
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return tl, nil
}
