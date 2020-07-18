package compattest

import (
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

func (db *WhispertoolDB) ArciveInfoList() whispertool.ArchiveInfoList {
	return db.db.ArchiveInfoList()
}

func (db *WhispertoolDB) Update(t whispertool.Timestamp, value whispertool.Value) error {
	return db.db.Update(t, value)
}

func (db *WhispertoolDB) UpdatePointsForArchive(points []whispertool.Point, archiveID int) error {
	return db.db.UpdatePointsForArchive(points, archiveID, 0)
}

func (db *WhispertoolDB) Sync() error {
	return db.db.Sync()
}

func (db *WhispertoolDB) Fetch(from, until whispertool.Timestamp) (*whispertool.TimeSeries, error) {
	return db.db.Fetch(from, until)
}
