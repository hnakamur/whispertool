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

func (db *GoWhisperDB) UpdatePointsForArchive(points []whispertool.Point, archiveID int) error {
	return db.db.UpdateManyForArchive(
		convertWhispertoolsPointsToToGoWhisperTimeSeriesPointPointers(points),
		db.db.Retentions()[archiveID].MaxRetention())
}

func (db *GoWhisperDB) Sync() error {
	return nil
}

func (db *GoWhisperDB) Fetch(from, until time.Time) (*whispertool.TimeSeries, error) {
	ts, err := db.db.Fetch(int(from.Unix()), int(until.Unix()))
	if err != nil {
		return nil, err
	}
	return convertGoWhisperTimeSeriesToWhispertoolTimeSeries(ts), nil
}

func convertWhispertoolsPointsToToGoWhisperTimeSeriesPointPointers(pts []whispertool.Point) []*whisper.TimeSeriesPoint {
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

func convertGoWhisperTimeSeriesToWhispertoolTimeSeries(ts *whisper.TimeSeries) *whispertool.TimeSeries {
	if ts == nil {
		return nil
	}
	from := whispertool.Timestamp(ts.FromTime())
	until := whispertool.Timestamp(ts.UntilTime())
	step := whispertool.Duration(ts.Step())
	var values []whispertool.Value
	if ts.Values() != nil {
		values = make([]whispertool.Value, len(ts.Values()))
		for i, v := range ts.Values() {
			values[i] = whispertool.Value(v)
		}
	}
	return whispertool.NewTimeSeries(from, until, step, values)
}
