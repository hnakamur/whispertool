package whispertool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	uint32Size          = 4
	uint64Size          = 8
	float32Size         = uint32Size
	float64Size         = uint64Size
	metaSize            = 3*uint32Size + float32Size
	archiveInfoListSize = 3 * uint32Size
	pointSize           = uint32Size + float64Size
)

// Header respresents a whisper file header.
type Header struct {
	aggregationMethod AggregationMethod
	maxRetention      Duration
	xFilesFactor      float32
	archiveCount      uint32
	archiveInfoList   ArchiveInfoList
}

// NewHeader creates and retruns a whisper file header.
// It returns an error if validations of passed arguments failed.
func NewHeader(aggregationMethod AggregationMethod, xFilesFactor float32, archiveInfoList ArchiveInfoList) (*Header, error) {
	if err := validateAggregationMethod(aggregationMethod); err != nil {
		return nil, err
	}
	if err := validateXFilesFactor(xFilesFactor); err != nil {
		return nil, err
	}

	archiveInfoList.fillOffset()
	if err := archiveInfoList.validate(); err != nil {
		return nil, err
	}

	h := &Header{
		aggregationMethod: aggregationMethod,
		maxRetention:      archiveInfoList[len(archiveInfoList)-1].MaxRetention(),
		xFilesFactor:      xFilesFactor,
		archiveCount:      uint32(len(archiveInfoList)),
		archiveInfoList:   archiveInfoList,
	}

	return h, nil
}

// AggregationMethod returns the aggregation method of the whisper file.
func (h *Header) AggregationMethod() AggregationMethod { return h.aggregationMethod }

// XFilesFactor returns the xFilesFactor of the whisper file.
func (h *Header) XFilesFactor() float32 { return h.xFilesFactor }

// MaxRetention returns the max retention of the whisper file.
func (h *Header) MaxRetention() Duration { return h.maxRetention }

// ArchiveInfoList returns the archive info list of the whisper file.
func (h *Header) ArchiveInfoList() ArchiveInfoList { return h.archiveInfoList }

// ByteSize returns the size in bytes of h in the whisper file.
func (h *Header) Size() int64 {
	return metaSize + int64(h.archiveCount)*archiveInfoListSize
}

// ExpectedFileSize returns the expected file size from h.
func (h *Header) ExpectedFileSize() int64 {
	sz := h.Size()
	for _, a := range h.archiveInfoList {
		sz += int64(a.numberOfPoints) * pointSize
	}
	return sz
}

// String returns the string representation of h in LTSV format [1].
//
// [1] Labeled Tab-separated Values http://ltsv.org/
func (h *Header) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "aggMethod:%s\taggMethodNum:%d\tmaxRetention:%s\txFileFactor:%s\tarchiveCount:%d\n",
		h.aggregationMethod,
		int(h.aggregationMethod),
		h.maxRetention,
		strconv.FormatFloat(float64(h.xFilesFactor), 'f', -1, 32),
		h.archiveCount)

	for i := range h.archiveInfoList {
		r := &h.ArchiveInfoList()[i]
		fmt.Fprintf(&b, "archiveInfo:%d\tdurationPerPoint:%s\tnumberOfPoints:%d\toffset:%d\n",
			i,
			Duration(r.secondsPerPoint),
			r.numberOfPoints,
			r.offset)
	}
	return b.String()
}

// AppendTo appends encoded bytes of h to dst
// and returns the extended buffer.
//
// AppendTo method implements the AppenderTo interface.
func (h *Header) AppendTo(dst []byte) []byte {
	var b [uint32Size]byte

	binary.BigEndian.PutUint32(b[:], uint32(h.aggregationMethod))
	dst = append(dst, b[:]...)

	dst = h.maxRetention.AppendTo(dst)

	binary.BigEndian.PutUint32(b[:], math.Float32bits(h.xFilesFactor))
	dst = append(dst, b[:]...)

	binary.BigEndian.PutUint32(b[:], h.archiveCount)
	dst = append(dst, b[:]...)

	for i := range h.archiveInfoList {
		dst = h.archiveInfoList[i].AppendTo(dst)
	}
	return dst
}

// TakeFrom updates ts from encoded bytes in src
// and returns the rest of src.
//
// TakeFrom method implements the TakerFrom interface.
// If there is an error, it may be of type *WantLargerBufferError.
func (h *Header) TakeFrom(src []byte) ([]byte, error) {
	if len(src) < metaSize {
		return nil, &WantLargerBufferError{WantedBufSize: metaSize}
	}

	h.aggregationMethod = AggregationMethod(binary.BigEndian.Uint32(src))
	src = src[uint32Size:]

	src, err := h.maxRetention.TakeFrom(src)
	if err != nil {
		return nil, err
	}

	h.xFilesFactor = math.Float32frombits(binary.BigEndian.Uint32(src))
	src = src[uint32Size:]

	h.archiveCount = binary.BigEndian.Uint32(src)
	src = src[uint32Size:]

	if err := validateAggregationMethod(h.aggregationMethod); err != nil {
		return nil, err
	}
	if err := validateXFilesFactor(h.xFilesFactor); err != nil {
		return nil, err
	}

	wantedSize := int(h.archiveCount * archiveInfoListSize)
	if len(src) < wantedSize {
		return nil, &WantLargerBufferError{WantedBufSize: metaSize + wantedSize}
	}

	h.archiveInfoList = make(ArchiveInfoList, h.archiveCount)
	for i := range h.archiveInfoList {
		src, err = h.archiveInfoList[i].TakeFrom(src)
		if err != nil {
			return nil, err
		}
	}
	if err := h.archiveInfoList.validate(); err != nil {
		return nil, err
	}

	return src, nil
}

func validateAggregationMethod(aggMethod AggregationMethod) error {
	switch aggMethod {
	case Average, Sum, Last, Max, Min, First:
		return nil
	default:
		return errors.New("invalid aggregation method")
	}
}

func validateXFilesFactor(xFilesFactor float32) error {
	if xFilesFactor < 0 || 1 < xFilesFactor {
		return errors.New("invalid XFilesFactor")
	}
	return nil
}
