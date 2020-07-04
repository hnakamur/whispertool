package whispertool

import (
	"encoding/binary"
	"math"
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
	if err := archiveInfoList.validate(); err != nil {
		return nil, err
	}

	return &Header{
		aggregationMethod: aggregationMethod,
		maxRetention:      archiveInfoList[len(archiveInfoList)-1].MaxRetention(),
		xFilesFactor:      xFilesFactor,
		archiveCount:      uint32(len(archiveInfoList)),
		archiveInfoList:   archiveInfoList,
	}, nil
}

// AggregationMethod returns the aggregation method of the whisper file.
func (h *Header) AggregationMethod() AggregationMethod { return h.aggregationMethod }

// XFilesFactor returns the xFilesFactor of the whisper file.
func (h *Header) XFilesFactor() float32 { return h.xFilesFactor }

// MaxRetention returns the max retention of the whisper file.
func (h *Header) MaxRetention() Duration { return h.maxRetention }

// ArchiveInfoList returns the archive info list of the whisper file.
func (h *Header) ArchiveInfoList() ArchiveInfoList { return h.archiveInfoList }

// AppendTo appends encoded bytes of h to dst
// and returns the extended buffer.
//
// AppendTo method implements the AppenderTo interface.
func (h *Header) AppendTo(dst []byte) []byte {
	var b [uint32Size]byte

	binary.BigEndian.PutUint32(b[:], uint32(h.aggregationMethod))
	dst = append(dst, b[:]...)

	binary.BigEndian.PutUint32(b[:], math.Float32bits(h.xFilesFactor))
	dst = append(dst, b[:]...)

	dst = h.maxRetention.AppendTo(dst)

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
		return nil, &WantLargerBufferError{WantedByteLen: metaSize - len(src)}
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

	wantSize := int(h.archiveCount * archiveInfoListSize)
	if len(src) < wantSize {
		return nil, &WantLargerBufferError{WantedByteLen: wantSize - len(src)}
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
