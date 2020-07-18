package compattest

import (
	"errors"
	"time"

	"github.com/go-graphite/go-whisper"
	"github.com/hnakamur/whispertool"
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

	db, err := whisper.Create(filename, retentions, aggMethod, xFilesFactor)
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

func (db *GoWhisperDB) Update(t time.Time, value float64) error {
	return db.db.Update(value, int(t.Unix()))
}

func (db *GoWhisperDB) UpdatePointsForArchive(points []Point, archiveID int) error {
	return db.db.UpdateManyForArchive(
		convertToGoWhisperTimeSeriesPointPointers(points),
		db.db.Retentions()[archiveID].MaxRetention())
}

func (db *GoWhisperDB) Sync() error {
	return nil
}

func (db *GoWhisperDB) Fetch(from, until time.Time) (*TimeSeries, error) {
	ts, err := db.db.Fetch(int(from.Unix()), int(until.Unix()))
	if err != nil {
		return nil, err
	}
	return convertGoWhisperTimeSeries(ts), nil
}

func convertToGoWhisperTimeSeriesPointPointers(pts []Point) []*whisper.TimeSeriesPoint {
	if pts == nil {
		return nil
	}
	points := make([]*whisper.TimeSeriesPoint, len(pts))
	for i, p := range pts {
		points[i] = &whisper.TimeSeriesPoint{
			Time:  int(p.Time.Unix()),
			Value: p.Value,
		}
	}
	return points
}

func convertGoWhisperTimeSeries(ts *whisper.TimeSeries) *TimeSeries {
	if ts == nil {
		return nil
	}
	from := time.Unix(int64(ts.FromTime()), 0)
	until := time.Unix(int64(ts.UntilTime()), 0)
	step := time.Duration(ts.Step())
	return &TimeSeries{
		from:   from,
		until:  until,
		step:   step,
		values: ts.Values(),
	}
}
