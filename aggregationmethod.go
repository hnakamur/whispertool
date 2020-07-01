//go:generate enumer -type AggregationMethod -transform=snake

// NOTE: You can install enumer by running
// go get github.com/alvaroloes/enumer

package whispertool

// AggregationMethod is the type of aggregation used in a whisper database.
// Note: 4 bytes long in Whisper Header, 1 byte long in Archive Header
type AggregationMethod int

// AggregationMethod constants
const (
	Average AggregationMethod = iota + 1
	Sum
	Last
	Max
	Min
	First

	Mix        // only used in whisper header
	Percentile // only used in archive header
)
