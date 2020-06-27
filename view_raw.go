package whispertool

import (
	"fmt"
	"strconv"
	"time"
)

func viewRaw(filename string, from, until time.Time, retId int, showHeader bool) error {
	d, err := ReadFile(filename)
	if err != nil {
		return err
	}
	tsFrom := TimestampFromStdTime(from)
	tsUntil := TimestampFromStdTime(until)

	m := d.Meta
	if showHeader {
		fmt.Printf("aggMethod:%s\taggMethodNum:%d\tmaxRetention:%s\txFileFactor:%s\tretentionCount:%d\n",
			m.AggregationMethod,
			int(m.AggregationMethod),
			m.maxRetention,
			strconv.FormatFloat(float64(m.XFilesFactor), 'f', -1, 32),
			m.retentionCount)
	}

	retentions := d.Retentions
	for i := range retentions {
		r := &retentions[i]
		if showHeader {
			fmt.Printf("retentionDef:%d\tstep:%s\tnumberOfPoints:%d\toffset:%d\n",
				i,
				Duration(r.SecondsPerPoint),
				r.NumberOfPoints,
				r.offset)
		}
	}
	for i := 0; i < len(retentions); i++ {
		if retId != RetIdAll && retId != i {
			continue
		}
		points := d.getAllRawUnsortedPoints(i)
		for j := 0; j < len(points); j++ {
			t := points[j].Time
			if t < tsFrom || tsUntil < t {
				continue
			}

			fmt.Printf("retId:%d\tpointIdx:%d\tt:%s\tval:%s\n",
				i, j, t, points[j].Value)
		}
	}
	return nil
}
