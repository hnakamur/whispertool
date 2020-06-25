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

func viewRaw(filename string, now, from, until time.Time, retId int, showHeader bool) error {
	fromUnix := uint32(from.Unix())
	untilUnix := uint32(until.Unix())

	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	m := &metadata{}
	err = m.readFrom(f)
	if err != nil {
		return err
	}
	if showHeader {
		fmt.Printf("aggMethod:%s\taggMethodNum:%d\tmaxRetention:%s\txFileFactor:%s\tretentionCount:%d\n",
			AggregationMethod(m.aggType),
			m.aggType,
			Duration(m.maxRetention),
			strconv.FormatFloat(float64(m.xFilesFactor), 'f', -1, 32),
			m.retentionCount)
	}

	retentions := make([]retention, m.retentionCount)
	for i := range retentions {
		r := &retentions[i]
		err = r.readFrom(f)
		if err != nil {
			return err
		}
		if showHeader {
			fmt.Printf("retentionDef:%d\tstep:%s\tnumberOfPoints:%d\toffset:%d\n",
				i,
				Duration(r.secondsPerPoint),
				r.numberOfPoints,
				r.offset)
		}
	}
	dataPoints := make([][]dataPoint, len(retentions))
	for i := 0; i < len(retentions); i++ {
		if retId != RetIdAll && retId != i {
			continue
		}
		dataPoints[i] = make([]dataPoint, retentions[i].numberOfPoints)
		for j := 0; j < int(retentions[i].numberOfPoints); j++ {
			err = dataPoints[i][j].readFrom(f)
			if err != nil {
				return err
			}

			t := dataPoints[i][j].interval
			if t < fromUnix || untilUnix < t {
				continue
			}

			fmt.Printf("retId:%d\tpointIdx:%d\tt:%s\tval:%s\n",
				i,
				j,
				formatTime(secondsToTime(int64(t))),
				strconv.FormatFloat(dataPoints[i][j].value, 'f', -1, 64))
		}
	}
	return nil
}

type metadata struct {
	aggType        uint32
	maxRetention   uint32
	xFilesFactor   float32
	retentionCount uint32
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

type retention struct {
	offset          uint32
	secondsPerPoint uint32
	numberOfPoints  uint32
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

type dataPoint struct {
	interval uint32
	value    float64
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
