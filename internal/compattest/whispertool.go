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

func (db *WhispertoolDB) UpdatePointsForArchive(points []whispertool.Point, archiveID int) error {
	return db.db.UpdatePointsForArchive(points, archiveID, 0)
}

func (db *WhispertoolDB) Sync() error {
	return db.db.Sync()
}

func (db *WhispertoolDB) Fetch(from, until time.Time) (*whispertool.TimeSeries, error) {
	return db.db.FetchFromArchive(
		whispertool.ArchiveIDBest,
		whispertool.TimestampFromStdTime(from),
		whispertool.TimestampFromStdTime(until),
		0)
}
