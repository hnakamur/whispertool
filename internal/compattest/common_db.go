package compattest

import (
	"github.com/hnakamur/whispertool"
	"github.com/hnakamur/whispertool/cmd"
	"golang.org/x/sync/errgroup"
)

type CommonDB interface {
	ArciveInfoList() whispertool.ArchiveInfoList
	Fetch(from, until whispertool.Timestamp) (*whispertool.TimeSeries, error)
	Update(t whispertool.Timestamp, value whispertool.Value) error
	UpdatePointsForArchive(points []whispertool.Point, archiveID int) error
	Sync() error
	Close() error
}

func BothUpdate(db1, db2 CommonDB, t whispertool.Timestamp, value whispertool.Value) error {
	var eg errgroup.Group
	eg.Go(func() error {
		return db1.Update(t, value)
	})
	eg.Go(func() error {
		return db2.Update(t, value)
	})
	return eg.Wait()
}

func BothUpdatePointsForArchive(db1, db2 CommonDB, points []whispertool.Point, archiveID int) error {
	var eg errgroup.Group
	eg.Go(func() error {
		return db1.UpdatePointsForArchive(points, archiveID)
	})
	eg.Go(func() error {
		return db2.UpdatePointsForArchive(points, archiveID)
	})
	return eg.Wait()
}
func BothSync(db1, db2 CommonDB) error {
	var eg errgroup.Group
	eg.Go(db1.Sync)
	eg.Go(db2.Sync)
	return eg.Wait()
}

func BothClose(db1, db2 CommonDB) error {
	var eg errgroup.Group
	eg.Go(db1.Close)
	eg.Go(db2.Close)
	return eg.Wait()
}

func BothFetchAllArchives(db1, db2 CommonDB, now whispertool.Timestamp) (ts1, ts2 cmd.TimeSeriesList, err error) {
	var eg errgroup.Group
	eg.Go(func() error {
		var err error
		ts1, err = FetchAllArchives(db1, now)
		return err
	})
	eg.Go(func() error {
		var err error
		ts2, err = FetchAllArchives(db2, now)
		return err
	})
	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}
	return ts1, ts2, nil
}

func FetchAllArchives(db CommonDB, now whispertool.Timestamp) (cmd.TimeSeriesList, error) {
	tl := make(cmd.TimeSeriesList, len(db.ArciveInfoList()))
	for archiveID, archiveInfo := range db.ArciveInfoList() {
		until := now
		from := now.Add(-archiveInfo.MaxRetention())
		ts, err := db.Fetch(from, until)
		if err != nil {
			return nil, err
		}
		tl[archiveID] = ts
	}
	return tl, nil
}
