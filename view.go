package whispertool

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"time"
)

func View(filename string, raw bool) error {
	if raw {
		return viewRaw(filename)
	}
	return nil
}

func viewRaw(filename string) error {
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
	fmt.Printf("aggType:%d\tmaxRetention:%s\txFileFactor:%g\tretentionCount:%d\n",
		m.aggType,
		secondsToDuration(int64(m.maxRetention)),
		m.xFilesFactor,
		m.retentionCount)

	retentions := make([]retention, m.retentionCount)
	for i := range retentions {
		r := &retentions[i]
		err = r.readFrom(f)
		if err != nil {
			return err
		}
		fmt.Printf("retentionDef:%d\tretentionStep:%s\tnumberOfPoints:%d\toffset:%d\n",
			i,
			secondsToDuration(int64(r.secondsPerPoint)),
			r.numberOfPoints,
			r.offset)
	}
	dataPoints := make([][]dataPoint, len(retentions))
	for i := 0; i < len(retentions); i++ {
		dataPoints[i] = make([]dataPoint, retentions[i].numberOfPoints)
		for j := 0; j < int(retentions[i].numberOfPoints); j++ {
			err = dataPoints[i][j].readFrom(f)
			if err != nil {
				return err
			}
			t := time.Unix(int64(dataPoints[i][j].interval), 0)
			fmt.Printf("retentionId:%d\tpointId:%d\ttime:%s\tvalue:%g\n",
				i, j, t.UTC().Format("2006-01-02T15:04:05Z"), dataPoints[i][j].value)
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
	return math.Float64frombits(intVal), nil
}

func secondsToDuration(d int64) time.Duration {
	return time.Duration(d) * time.Second
}
