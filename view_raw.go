package whispertool

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"time"
)

const UTCTimeLayout = "2006-01-02T15:04:05Z"

type rawFileData struct {
	filename   string
	meta       *metadata
	retentions []retention
	pointsList [][]dataPoint
}

type metadata struct {
	aggType        uint32
	maxRetention   uint32
	xFilesFactor   float32
	retentionCount uint32
}

type retention struct {
	offset          uint32
	secondsPerPoint uint32
	numberOfPoints  uint32
}

type dataPoint struct {
	interval uint32
	value    float64
}

func viewRaw(filename string, now, from, until time.Time, retId int, showHeader bool) (*rawFileData, error) {
	d, err := readRawFileData(filename)
	if err != nil {
		return nil, err
	}
	fromUnix := uint32(from.Unix())
	untilUnix := uint32(until.Unix())

	m := d.meta
	if showHeader {
		fmt.Printf("aggMethod:%s\taggMethodNum:%d\tmaxRetention:%s\txFileFactor:%s\tretentionCount:%d\n",
			AggregationMethod(m.aggType),
			m.aggType,
			Duration(m.maxRetention),
			strconv.FormatFloat(float64(m.xFilesFactor), 'f', -1, 32),
			m.retentionCount)
	}

	retentions := d.retentions
	for i := range retentions {
		r := &retentions[i]
		if showHeader {
			fmt.Printf("retentionDef:%d\tstep:%s\tnumberOfPoints:%d\toffset:%d\n",
				i,
				Duration(r.secondsPerPoint),
				r.numberOfPoints,
				r.offset)
		}
	}
	for i := 0; i < len(d.pointsList); i++ {
		if retId != RetIdAll && retId != i {
			continue
		}
		points := d.pointsList[i]
		for j := 0; j < len(points); j++ {
			t := points[j].interval
			if t < fromUnix || untilUnix < t {
				continue
			}

			fmt.Printf("retId:%d\tpointIdx:%d\tt:%s\tval:%s\n",
				i,
				j,
				formatTime(secondsToTime(int64(t))),
				strconv.FormatFloat(points[j].value, 'f', -1, 64))
		}
	}
	return d, nil
}

func readRawFileData(filename string) (*rawFileData, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	m := &metadata{}
	err = m.readFrom(f)
	if err != nil {
		return nil, err
	}

	retentions := make([]retention, m.retentionCount)
	for i := range retentions {
		r := &retentions[i]
		err = r.readFrom(f)
		if err != nil {
			return nil, err
		}
	}
	pointsList := make([][]dataPoint, len(retentions))
	for i := 0; i < len(retentions); i++ {
		pointsList[i] = make([]dataPoint, retentions[i].numberOfPoints)
		for j := 0; j < int(retentions[i].numberOfPoints); j++ {
			err = pointsList[i][j].readFrom(f)
			if err != nil {
				return nil, err
			}
		}
	}
	return &rawFileData{
		filename:   filename,
		meta:       m,
		retentions: retentions,
		pointsList: pointsList,
	}, nil
}

func (m *metadata) readFrom(r io.Reader) error {
	err := binary.Read(r, binary.BigEndian, &m.aggType)
	if err != nil {
		return err
	}
	err = binary.Read(r, binary.BigEndian, &m.maxRetention)
	if err != nil {
		return err
	}
	m.xFilesFactor, err = readFloat32From(r)
	if err != nil {
		return err
	}
	return binary.Read(r, binary.BigEndian, &m.retentionCount)
}

func (rt *retention) readFrom(r io.Reader) error {
	err := binary.Read(r, binary.BigEndian, &rt.offset)
	if err != nil {
		return err
	}
	err = binary.Read(r, binary.BigEndian, &rt.secondsPerPoint)
	if err != nil {
		return err
	}
	return binary.Read(r, binary.BigEndian, &rt.numberOfPoints)
}

func (p *dataPoint) readFrom(r io.Reader) error {
	err := binary.Read(r, binary.BigEndian, &p.interval)
	if err != nil {
		return err
	}
	v, err := readFloat64From(r)
	if err != nil {
		return err
	}
	p.value = v
	return nil
}

func readFloat32From(r io.Reader) (float32, error) {
	var intVal uint32
	err := binary.Read(r, binary.BigEndian, &intVal)
	if err != nil {
		return float32(math.NaN()), err
	}
	return math.Float32frombits(intVal), nil
}

func readFloat64From(r io.Reader) (float64, error) {
	var intVal uint64
	err := binary.Read(r, binary.BigEndian, &intVal)
	if err != nil {
		return math.NaN(), err
	}
	v := math.Float64frombits(intVal)
	return v, nil
}

func secondsToTime(t int64) time.Time {
	return time.Unix(t, 0)
}

func formatTime(t time.Time) string {
	return t.UTC().Format(UTCTimeLayout)
}

func convertRawDataToWhisperFileData(filename string, meta *metadata, retentions []retention, pointsList [][]dataPoint) *whisperFileData {
	return &whisperFileData{
		filename:          filename,
		aggregationMethod: AggregationMethod(meta.aggType),
		xFilesFactor:      meta.xFilesFactor,
		retentions:        convertRawRetentionsToWhisperRetentions(retentions),
		pointsList:        convertRawPointsListToWhisperPointsList(pointsList),
	}
}

func convertRawRetentionsToWhisperRetentions(retentions []retention) []Retention {
	whisperRetentions := make([]Retention, len(retentions))
	for i, r := range retentions {
		whisperRetentions[i] = Retention{
			SecondsPerPoint: Duration(r.secondsPerPoint),
			NumberOfPoints:  r.numberOfPoints,
		}
	}
	return whisperRetentions
}

func convertRawPointsListToWhisperPointsList(pointsList [][]dataPoint) [][]Point {
	whisperPointsList := make([][]Point, len(pointsList))
	for i, points := range pointsList {
		whisperPointsList[i] = make([]Point, len(points))
		for j, p := range points {
			whisperPointsList[i][j] = Point{
				Time:  Timestamp(p.interval),
				Value: Value(p.value),
			}
		}
	}
	return whisperPointsList
}
