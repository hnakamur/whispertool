package whispertool

import (
	"log"
	"math/rand"
	"os"
	"time"
)

func Hole(src, dest string, perm os.FileMode, textOut string, emptyRate float64, now, from, until time.Time) error {
	tsNow := TimestampFromStdTime(now)
	tsFrom := TimestampFromStdTime(from)
	tsUntil := TimestampFromStdTime(until)

	srcData, srcPtsList, err := readWhisperFile(src, tsNow, tsFrom, tsUntil, RetIDAll)
	if err != nil {
		return err
	}

	rnd := rand.New(rand.NewSource(newRandSeed()))
	destPtsList := emptyRandomPointsList(srcPtsList, rnd, emptyRate, tsFrom, tsUntil, srcData.Retentions)
	destData, err := NewFileData(srcData.Meta, srcData.Retentions)
	if err != nil {
		return err
	}

	if err := updateFileDataWithPointsList(destData, destPtsList, tsNow); err != nil {
		return err
	}

	if err = printFileData(textOut, destData, destPtsList, true); err != nil {
		return err
	}

	if err := WriteFile(dest, destData, perm); err != nil {
		return err
	}
	return nil
}

func emptyRandomPointsList(ptsList [][]Point, rnd *rand.Rand, emptyRate float64, from, until Timestamp, retentions []Retention) [][]Point {
	ptsList2 := make([][]Point, len(ptsList))
	for i, pts := range ptsList {
		r := &retentions[i]
		ptsList2[i] = emptyRandomPoints(pts, rnd, emptyRate, from, until, r)
	}
	return ptsList2
}

func emptyRandomPoints(pts []Point, rnd *rand.Rand, empyRate float64, from, until Timestamp, r *Retention) []Point {
	var pts2 []Point
	for _, p := range pts {
		if from < p.Time && p.Time <= until && rnd.Float64() < empyRate {
			log.Printf("skip r=%s, p.Time=%s", r, p.Time)
			continue
		}
		pts2 = append(pts2, Point{
			Time:  p.Time,
			Value: p.Value,
		})
	}
	return pts2
}
