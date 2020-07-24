package compattest

import (
	"fmt"

	"github.com/hnakamur/whispertool"
	"pgregory.net/rapid"
)

type Points whispertool.Points

func (pp Points) Format(f fmt.State, c rune) {
	f.Write([]byte(whispertool.Points(pp).String()))
}

type TimestampRange struct {
	From  whispertool.Timestamp
	Until whispertool.Timestamp
}

func (r TimestampRange) String() string {
	return "{" + r.From.String() + " " + r.Until.String() + "}"
}

func (r TimestampRange) Format(f fmt.State, c rune) {
	f.Write([]byte(r.String()))
}

func NewPointGenerator(db *WhispertoolDB) *rapid.Generator {
	return rapid.Custom(func(t *rapid.T) whispertool.Point {
		now := whispertool.TimestampFromStdTime(clock.Now())
		oldest := now.Add(-(db.db.MaxRetention() - db.ArciveInfoList()[0].SecondsPerPoint()))
		timestamp := whispertool.Timestamp(rapid.Uint32Range(uint32(oldest), uint32(now)).Draw(t, "timestamp").(uint32))
		v := whispertool.Value(rapid.Float64().Draw(t, "v").(float64))
		return whispertool.Point{Time: timestamp, Value: v}
	})
}

func NewPointsForArchiveGenerator(db *WhispertoolDB, archiveID int) *rapid.Generator {
	return rapid.Custom(func(t *rapid.T) Points {
		var points Points
		now := whispertool.TimestampFromStdTime(clock.Now())
		archiveInfo := db.ArciveInfoList()[archiveID]
		step := archiveInfo.SecondsPerPoint()
		oldest := now.Add(-archiveInfo.MaxRetention()).Add(step)
		fillRatio := rapid.Float64Range(0, 1).Draw(t, "fillRatio").(float64)
		for timestamp := oldest; timestamp <= now; timestamp = timestamp.Add(step) {
			ptFillRatio := rapid.Float64Range(0, 1).Draw(t, "ptFillRatio").(float64)
			if ptFillRatio < fillRatio {
				v := rapid.Float64().Draw(t, "v").(float64)
				points = append(points, whispertool.Point{Time: timestamp, Value: whispertool.Value(v)})
			}
		}
		return points
	})
}

func NewAllFilledPointsForArchiveGenerator(db *WhispertoolDB, archiveID int) *rapid.Generator {
	return rapid.Custom(func(t *rapid.T) Points {
		var points Points
		now := whispertool.TimestampFromStdTime(clock.Now())
		archiveInfo := db.ArciveInfoList()[archiveID]
		step := archiveInfo.SecondsPerPoint()
		oldest := now.Add(-archiveInfo.MaxRetention()).Add(step)
		for timestamp := oldest; timestamp <= now; timestamp = timestamp.Add(step) {
			v := rapid.Float64().Draw(t, "v").(float64)
			points = append(points, whispertool.Point{Time: timestamp, Value: whispertool.Value(v)})
		}
		return points
	})
}

func NewPointsForAllArchivesGenerator(db *WhispertoolDB) *rapid.Generator {
	return rapid.Custom(func(t *rapid.T) Points {
		var points Points
		now := whispertool.TimestampFromStdTime(clock.Now())
		for _, archiveInfo := range db.ArciveInfoList() {
			step := archiveInfo.SecondsPerPoint()
			oldest := now.Add(-archiveInfo.MaxRetention()).Add(step)
			fillRatio := rapid.Float64Range(0, 1).Draw(t, "fillRatio").(float64)
			for timestamp := oldest.Add(step); timestamp <= now; timestamp = timestamp.Add(step) {
				ptFillRatio := rapid.Float64Range(0, 1).Draw(t, "ptFillRatio").(float64)
				if ptFillRatio < fillRatio {
					v := rapid.Float64().Draw(t, "v").(float64)
					points = append(points, whispertool.Point{Time: timestamp, Value: whispertool.Value(v)})
				}
			}
		}
		return points
	})
}

func NewTimestampRangeGenerator(db *WhispertoolDB) *rapid.Generator {
	return rapid.Custom(func(t *rapid.T) TimestampRange {
		now := whispertool.TimestampFromStdTime(clock.Now())
		oldest := now.Add(-db.db.MaxRetention())

		step := db.ArciveInfoList()[0].SecondsPerPoint()
		min := oldest.Add(-step)
		max := now.Add(step)

		from := whispertool.Timestamp(rapid.Uint32Range(uint32(min), uint32(max)).Draw(t, "from").(uint32))
		until := whispertool.Timestamp(rapid.Uint32Range(uint32(min), uint32(max)).Draw(t, "until").(uint32))
		if until < from {
			from, until = until, from
		}
		return TimestampRange{From: from, Until: until}
	})
}
