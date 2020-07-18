package compattest

import (
	"time"

	"github.com/hnakamur/whispertool"
)

type WhispertoolDB struct {
	db *whispertool.Whisper
}

func CreateWhispertoolDB(filename string, retentionDefs string, aggregationMethod string, xFilesFactor float32) (*WhispertoolDB, error) {
	archiveInfoList, err := whispertool.ParseArchiveInfoList(retentionDefs)
	if err != nil {
		return nil, err
	}

	aggMethod, err := whispertool.AggregationMethodString(aggregationMethod)
	if err != nil {
		return nil, err
	}

	db, err := whispertool.Create(filename, archiveInfoList, aggMethod, xFilesFactor)
	if err != nil {
		return nil, err
	}
	return &WhispertoolDB{db: db}, nil
}

func OpenWhispertoolDB(filename string) (*WhispertoolDB, error) {
	db, err := whispertool.Open(filename)
	if err != nil {
		return nil, err
	}
	return &WhispertoolDB{db: db}, nil
}

func (db *WhispertoolDB) Update(t time.Time, value float64) error {
	return db.db.Update(whispertool.TimestampFromStdTime(t), whispertool.Value(value))
}

func (db *WhispertoolDB) UpdatePointsForArchive(points []Point, archiveID int) error {
	return db.db.UpdatePointsForArchive(
		convertToGoWhispertoolPoints(points),
		archiveID,
		0)
}

func (db *WhispertoolDB) Sync() error {
	return db.db.Sync()
}

func (db *WhispertoolDB) Fetch(from, until time.Time) (*TimeSeries, error) {
	ts, err := db.db.FetchFromArchive(
		whispertool.ArchiveIDBest,
		whispertool.TimestampFromStdTime(from),
		whispertool.TimestampFromStdTime(until),
		0)
	if err != nil {
		return nil, err
	}
	return convertWhispertoolTimeSeries(ts), nil
}

func convertToGoWhispertoolPoints(pts []Point) []whispertool.Point {
	if pts == nil {
		return nil
	}
	points := make([]whispertool.Point, len(pts))
	for i, p := range pts {
		points[i] = whispertool.Point{
			Time:  whispertool.TimestampFromStdTime(p.Time),
			Value: whispertool.Value(p.Value),
		}
	}
	return points
}

func convertWhispertoolTimeSeries(ts *whispertool.TimeSeries) *TimeSeries {
	if ts == nil {
		return nil
	}
	from := ts.FromTime().ToStdTime()
	until := ts.UntilTime().ToStdTime()
	step := time.Duration(ts.Step()) * time.Second
	var values []float64
	if ts.Values() != nil {
		values = make([]float64, len(ts.Values()))
		for i, v := range ts.Values() {
			values[i] = float64(v)
		}
	}
	return &TimeSeries{
		from:   from,
		until:  until,
		step:   step,
		values: values,
	}
}
