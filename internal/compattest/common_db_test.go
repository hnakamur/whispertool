package compattest

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/hnakamur/whispertool"
	"github.com/hnakamur/whispertool/cmd"
	"golang.org/x/sync/errgroup"
)

type commonDB interface {
	ArciveInfoList() whispertool.ArchiveInfoList
	Fetch(from, until whispertool.Timestamp) (*whispertool.TimeSeries, error)
}

const (
	goWhisperFilename   = "go-whisper.wsp"
	whispertoolFilename = "whispertool.wsp"
)

func bothCreate(t *testing.T, dir, retentionDefs, aggregationMethod string, xFilesFactor float32) (db1 *WhispertoolDB, db2 *GoWhisperDB) {
	var eg errgroup.Group
	eg.Go(func() error {
		var err error
		db1, err = CreateWhispertoolDB(filepath.Join(dir, whispertoolFilename), retentionDefs, aggregationMethod, xFilesFactor)
		return err
	})
	eg.Go(func() error {
		var err error
		db2, err = CreateGoWhisperDB(filepath.Join(dir, goWhisperFilename), retentionDefs, aggregationMethod, xFilesFactor)
		return err
	})
	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
	return db1, db2
}

func bothOpen(t *testing.T, dir string) (db1 *WhispertoolDB, db2 *GoWhisperDB) {
	var eg errgroup.Group
	eg.Go(func() error {
		var err error
		db1, err = OpenWhispertoolDB(filepath.Join(dir, whispertoolFilename))
		return err
	})
	eg.Go(func() error {
		var err error
		db2, err = OpenGoWhisperDB(filepath.Join(dir, goWhisperFilename))
		return err
	})
	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
	return db1, db2
}

func bothUpdate(t *testing.T, db1 *WhispertoolDB, db2 *GoWhisperDB, timestamp whispertool.Timestamp, value whispertool.Value) {
	var eg errgroup.Group
	eg.Go(func() error {
		if err := db1.Update(timestamp, value); err != nil {
			return fmt.Errorf("whispertool: %s", err)
		}
		return nil
	})
	eg.Go(func() error {
		if err := db2.Update(timestamp, value); err != nil {
			return fmt.Errorf("go-whisper: %s", err)
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
}

func bothUpdatePointsForArchive(t *testing.T, db1 *WhispertoolDB, db2 *GoWhisperDB, points []whispertool.Point, archiveID int) {
	var eg errgroup.Group
	eg.Go(func() error {
		return db1.UpdatePointsForArchive(points, archiveID)
	})
	eg.Go(func() error {
		return db2.UpdatePointsForArchive(points, archiveID)
	})
	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
}

func bothSync(t *testing.T, db1 *WhispertoolDB, db2 *GoWhisperDB) {
	var eg errgroup.Group
	eg.Go(db1.Sync)
	eg.Go(db2.Sync)
	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
}

func bothClose(t *testing.T, db1 *WhispertoolDB, db2 *GoWhisperDB) {
	var eg errgroup.Group
	eg.Go(db1.Close)
	eg.Go(db2.Close)
	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
}

func bothFetchAllArchives(t *testing.T, db1 *WhispertoolDB, db2 *GoWhisperDB, now whispertool.Timestamp) (ts1, ts2 cmd.TimeSeriesList) {
	var eg errgroup.Group
	eg.Go(func() error {
		var err error
		ts1, err = fetchAllArchives(db1, now)
		return err
	})
	eg.Go(func() error {
		var err error
		ts2, err = fetchAllArchives(db2, now)
		return err
	})
	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
	return ts1, ts2
}

func fetchAllArchives(db commonDB, now whispertool.Timestamp) (cmd.TimeSeriesList, error) {
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
