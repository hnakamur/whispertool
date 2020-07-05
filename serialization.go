package whispertool

import "fmt"

// AppendTaker is the interface that groups
// AppendTo and TakeFrom methods.
type AppendTaker interface {
	AppenderTo
	TakerFrom
}

// AppenderTo is the interface for serializing
// an object and appends the encoded bytes to dst.
type AppenderTo interface {
	AppendTo(dst []byte) []byte
}

// TakerFrom is the interface for deserializing
// an object and returns the rest of src.
//
// If there is an error, it may be of type *WantLargerBufferError.
type TakerFrom interface {
	TakeFrom(src []byte) (rest []byte, err error)
}

// WantLargerBufferError records the length of bytes
// wanted to deserialize.
//
// If WantedByteLen is zero, it means the length
// wanted is unkown.
type WantLargerBufferError struct {
	WantedBufSize int
}

// Error returns the error message for e.
func (e *WantLargerBufferError) Error() string {
	if e.WantedBufSize == 0 {
		return "want larger buffer but size is unknown"
	}
	return fmt.Sprintf("want %d bytes larger buffer", e.WantedBufSize)
}
