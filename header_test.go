package whispertool

import (
	"bytes"
	"testing"
)

func TestHeaderTakeFromAppendTo(t *testing.T) {
	expected := []byte{
		// Metadata
		0x00, 0x00, 0x00, 0x01, // Aggregation type
		0x00, 0x00, 0x0e, 0x10, // Max retention
		0x3f, 0x00, 0x00, 0x00, // xFilesFactor
		0x00, 0x00, 0x00, 0x03, // Retention count
		// Archive Info
		// Retention 1 (1, 300)
		0x00, 0x00, 0x00, 0x34, // offset
		0x00, 0x00, 0x00, 0x01, // secondsPerPoint
		0x00, 0x00, 0x01, 0x2c, // numberOfPoints
		// Retention 2 (60, 30)
		0x00, 0x00, 0x0e, 0x44, // offset
		0x00, 0x00, 0x00, 0x3c, // secondsPerPoint
		0x00, 0x00, 0x00, 0x1e, // numberOfPoints
		// Retention 3 (300, 12)
		0x00, 0x00, 0x0f, 0xac, // offset
		0x00, 0x00, 0x01, 0x2c, // secondsPerPoint
		0x00, 0x00, 0x00, 0x0c} // numberOfPoints

	h := &Header{}
	rest, err := h.TakeFrom(expected)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(rest), 0; got != want {
		t.Errorf("rest length unmatch, got=%d, want=%d", got, want)
	}
	// log.Printf("decoded header=%v", h)

	if got, want := h.AppendTo(nil), expected; !bytes.Equal(got, want) {
		t.Errorf("encoded bytes unmatch")
	}
}
