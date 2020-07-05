package whispertool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

// Timestamp is the Unix timestamp, the number of seconds
// elapsed since January 1, 1970 UTC.
type Timestamp uint32

// Duration is seconds between two Timestamps.
type Duration int32

// Timestamp format layout used in this package.
// Note time zone is fixed to "Z".
const UTCTimeLayout = "2006-01-02T15:04:05Z"

// Duration constants
const (
	Second Duration = 1
	Minute          = 60 * Second
	Hour            = 60 * Minute
	Day             = 24 * Hour
	Week            = 7 * Day
	Year            = 365 * Day
)

// ParseTimestamp parses timestamp in "2006-01-02T15:04:05Z" format.
// Note only "Z" is allowed for timezone.
func ParseTimestamp(s string) (Timestamp, error) {
	t, err := time.Parse(UTCTimeLayout, s)
	if err != nil {
		return 0, fmt.Errorf("invalid timestamp: %s", err)
	}
	return TimestampFromStdTime(t), nil
}

// StdTimeToTimestamp returns t as a Timestamp.
func TimestampFromStdTime(t time.Time) Timestamp {
	if t.IsZero() {
		return Timestamp(0)
	}
	return Timestamp(t.UTC().Unix())
}

// ToStdTime returns t as a time.Time.
func (t Timestamp) ToStdTime() time.Time {
	return time.Unix(int64(t), 0).UTC()
}

// String returns the string representation of t with UTCTimeLayout format.
func (t Timestamp) String() string {
	return t.ToStdTime().Format(UTCTimeLayout)
}

// Add returns t+d.
func (t Timestamp) Add(d Duration) Timestamp {
	if d >= 0 {
		return t + Timestamp(d)
	}
	return t - Timestamp(-d)
}

// Sub returns the Duration t-u.
// To compute t-d for Duration, use t.Add(-d).
func (t Timestamp) Sub(u Timestamp) Duration {
	if t >= u {
		return Duration(t - u)
	}
	return -Duration(u - t)
}

// Truncate returns the result of rounding t down to a multiple of d (since the Unix epoch time). If d <= 0, Truncate returns t.
func (t Timestamp) Truncate(d Duration) Timestamp {
	if d <= 0 {
		return t
	}
	return t.Add(-Duration(int64(t) % int64(d)))
}

// AppendTo appends encoded bytes of t to dst
// and returns the extended buffer.
//
// AppendTo method implements the AppenderTo interface.
func (t *Timestamp) AppendTo(dst []byte) []byte {
	var b [uint32Size]byte
	binary.BigEndian.PutUint32(b[:], uint32(*t))
	return append(dst, b[:]...)
}

// TakeFrom updates t from encoded bytes in src
// and returns the rest of src.
//
// TakeFrom method implements the TakerFrom interface.
// If there is an error, it may be of type *WantLargerBufferError.
func (t *Timestamp) TakeFrom(src []byte) ([]byte, error) {
	if len(src) < uint32Size {
		return nil, &WantLargerBufferError{WantedBufSize: uint32Size}
	}
	*t = Timestamp(binary.BigEndian.Uint32(src[:uint32Size]))
	return src[uint32Size:], nil
}

// ParseDuration parses a Duration string.
func ParseDuration(s string) (Duration, error) {
	x, rem, err := leadingInt(s)
	if err != nil || len(rem) != 1 {
		return 0, fmt.Errorf("invalid Duration: %s", s)
	}
	unit, err := unitMultiplier(rem)
	if err != nil {
		return 0, fmt.Errorf("invalid Duration: %s: %s", s, err)
	}
	d := Duration(x)
	if d > math.MaxInt32/unit {
		// overflow
		return 0, fmt.Errorf("invalid Duration: %s", s)
	}
	d *= unit
	if d < 0 {
		// overflow
		return 0, fmt.Errorf("invalid Duration: %s", s)
	}
	return d, nil
}

var errLeadingInt = errors.New("Duration: bad [0-9]*") // never printed

func leadingInt(s string) (x int32, rem string, err error) {
	i := 0
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		if x > math.MaxInt32/10 {
			// overflow
			return 0, "", errLeadingInt
		}
		x = x*10 + int32(c) - '0'
		if x < 0 {
			// overflow
			return 0, "", errLeadingInt
		}
	}
	if x == 0 && i != 1 {
		// redundant leading zeros
		return 0, "", errLeadingInt
	}
	return x, s[i:], nil
}

func unitMultiplier(s string) (d Duration, err error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("invalid unit: %v", s)
	}
	switch s[0] {
	case 's':
		return Second, nil
	case 'm':
		return Minute, nil
	case 'h':
		return Hour, nil
	case 'd':
		return Day, nil
	case 'w':
		return Week, nil
	case 'y':
		return Year, nil
	default:
		return 0, fmt.Errorf("invalid unit: %v", s)
	}
}

// String returns the string representation of d.
func (d Duration) String() string {
	if d == 0 {
		return "0s"
	}
	switch {
	case d%Year == 0:
		return fmt.Sprintf("%dy", d/Year)
	case d%Week == 0:
		return fmt.Sprintf("%dw", d/Week)
	case d%Day == 0:
		return fmt.Sprintf("%dd", d/Day)
	case d%Hour == 0:
		return fmt.Sprintf("%dh", d/Hour)
	case d%Minute == 0:
		return fmt.Sprintf("%dm", d/Minute)
	default:
		return fmt.Sprintf("%ds", d)
	}
}

// AppendTo appends encoded bytes of d to dst
// and returns the extended buffer.
func (d *Duration) AppendTo(dst []byte) []byte {
	var b [uint32Size]byte
	binary.BigEndian.PutUint32(b[:], uint32(*d))
	return append(dst, b[:]...)
}

// TakeFrom updates d from encoded bytes in src
// and returns the rest of src.
//
// TakeFrom method implements the TakerFrom interface.
// If there is an error, it may be of type *WantLargerBufferError.
func (d *Duration) TakeFrom(src []byte) ([]byte, error) {
	if len(src) < uint32Size {
		return nil, &WantLargerBufferError{WantedBufSize: uint32Size}
	}
	*d = Duration(binary.BigEndian.Uint32(src[:uint32Size]))
	return src[uint32Size:], nil
}
