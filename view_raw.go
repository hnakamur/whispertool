package whispertool

import (
	"sort"
	"time"
)

func ViewRaw(filename string, from, until time.Time, retID int, textOut string, showHeader, sortsByTime bool) error {
	d, pointsList, err := readWhisperFileRaw(filename, retID)
	if err != nil {
		return err
	}

	tsFrom := TimestampFromStdTime(from)
	tsUntil := TimestampFromStdTime(until)
	pointsList = filterPointsListByTimeRange(d, pointsList, tsFrom, tsUntil)
	if sortsByTime {
		sortPointsListByTime(pointsList)
	}

	if err := printFileData(textOut, d, pointsList, showHeader); err != nil {
		return err
	}
	return nil
}

func readWhisperFileRaw(filename string, retID int) (*FileData, [][]Point, error) {
	d, err := ReadFile(filename)
	if err != nil {
		return nil, nil, err
	}

	pointsList := make([][]Point, len(d.Retentions))
	if retID == RetIdAll {
		for i := range d.Retentions {
			pointsList[i] = d.getAllRawUnsortedPoints(i)
		}
	} else if retID >= 0 && retID < len(d.Retentions) {
		pointsList[retID] = d.getAllRawUnsortedPoints(retID)
	} else {
		return nil, nil, ErrRetentionIDOutOfRange
	}
	return d, pointsList, nil
}

func filterPointsListByTimeRange(d *FileData, pointsList [][]Point, from, until Timestamp) [][]Point {
	pointsList2 := make([][]Point, len(pointsList))
	for i := range d.Retentions {
		r := &d.Retentions[i]
		pointsList2[i] = filterPointsByTimeRange(r, pointsList[i], from, until)
	}
	return pointsList2
}

func filterPointsByTimeRange(r *Retention, points []Point, from, until Timestamp) []Point {
	if until == from {
		until = until.Add(r.SecondsPerPoint)
	}
	var points2 []Point
	for _, p := range points {
		if p.Time <= from || p.Time > until {
			continue
		}
		points2 = append(points2, p)
	}
	return points2
}

func sortPointsListByTime(pointsList [][]Point) {
	for _, points := range pointsList {
		sort.Stable(Points(points))
	}
}
