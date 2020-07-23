package compattest

import (
	"fmt"
	"path/filepath"

	"github.com/hnakamur/whispertool"
	"github.com/hnakamur/whispertool/cmd"
	"golang.org/x/sync/errgroup"
)

const (
	goWhisperFilename   = "go-whisper.wsp"
	whispertoolFilename = "whispertool.wsp"
)

type tb interface {
	Fatal(args ...interface{})
}

func bothCreate(t tb, dir, retentionDefs, aggregationMethod string, xFilesFactor float32) (db1 *WhispertoolDB, db2 *GoWhisperDB) {
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

func bothOpen(t tb, dir string) (db1 *WhispertoolDB, db2 *GoWhisperDB) {
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

func bothUpdate(t tb, db1 *WhispertoolDB, db2 *GoWhisperDB, timestamp whispertool.Timestamp, value whispertool.Value) {
	var eg errgroup.Group
	eg.Go(func() error {
		if err := db1.Update(timestamp, value); err != nil {
			return fmt.Errorf("whispertool: %s", err)
		}
		return nil
	})
	eg.Go(func() error {
		if err := db2.Update(timestamp, value); err != nil {
			now := whispertool.TimestampFromStdTime(clock.Now())
			whispertoolOldest := now.Add(-db1.db.MaxRetention())
			goWhisperOldest := now.Add(-whispertool.Duration(db2.db.MaxRetention()))
			return fmt.Errorf("go-whisper: %s, whispertoolOldest=%s, goWhisperOldest=%s, now=%s, timestamp=%s", err, whispertoolOldest, goWhisperOldest, now, timestamp)
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
}

func bothUpdatePointsForArchive(t tb, db1 *WhispertoolDB, db2 *GoWhisperDB, points []whispertool.Point, archiveID int) {
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

func bothSync(t tb, db1 *WhispertoolDB, db2 *GoWhisperDB) {
	var eg errgroup.Group
	eg.Go(db1.Sync)
	eg.Go(db2.Sync)
	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
}

func bothClose(t tb, db1 *WhispertoolDB, db2 *GoWhisperDB) {
	var eg errgroup.Group
	eg.Go(db1.Close)
	eg.Go(db2.Close)
	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
}

func bothFetchAllArchives(t tb, db1 *WhispertoolDB, db2 *GoWhisperDB) (ts1, ts2 cmd.TimeSeriesList) {
	var eg errgroup.Group
	eg.Go(func() error {
		var err error
		ts1, err = db1.fetchAllArchives()
		return err
	})
	eg.Go(func() error {
		var err error
		ts2, err = db2.fetchAllArchives()
		return err
	})
	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
	return ts1, ts2
}
