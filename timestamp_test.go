package whispertool

import (
	"math"
	"testing"
	"time"
)

func TestParseDuration(t *testing.T) {
	testCases := []struct {
		input   string
		wantDur Duration
		wantErr bool
	}{
		{input: "0s", wantDur: 0, wantErr: false},
		{input: "0m", wantDur: 0, wantErr: false},
		{input: "0h", wantDur: 0, wantErr: false},
		{input: "0d", wantDur: 0, wantErr: false},
		{input: "0w", wantDur: 0, wantErr: false},
		{input: "0y", wantDur: 0, wantErr: false},

		{input: "1s", wantDur: Second, wantErr: false},
		{input: "1m", wantDur: Minute, wantErr: false},
		{input: "1h", wantDur: Hour, wantErr: false},
		{input: "1d", wantDur: Day, wantErr: false},
		{input: "1w", wantDur: Week, wantErr: false},
		{input: "1y", wantDur: Year, wantErr: false},

		{input: "10s", wantDur: 10 * Second, wantErr: false},
		{input: "10m", wantDur: 10 * Minute, wantErr: false},
		{input: "10h", wantDur: 10 * Hour, wantErr: false},
		{input: "10d", wantDur: 10 * Day, wantErr: false},
		{input: "10w", wantDur: 10 * Week, wantErr: false},
		{input: "10y", wantDur: 10 * Year, wantErr: false},

		// max values
		{input: "2147483647s", wantDur: 2147483647 * Second, wantErr: false},
		{input: "35791394m", wantDur: 35791394 * Minute, wantErr: false},
		{input: "596523h", wantDur: 596523 * Hour, wantErr: false},
		{input: "24855d", wantDur: 24855 * Day, wantErr: false},
		{input: "3550w", wantDur: 3550 * Week, wantErr: false},
		{input: "68y", wantDur: 68 * Year, wantErr: false},

		// overflow
		{input: "2147483648s", wantDur: 0, wantErr: true},
		{input: "35791395m", wantDur: 0, wantErr: true},
		{input: "596524h", wantDur: 0, wantErr: true},
		{input: "24856d", wantDur: 0, wantErr: true},
		{input: "3551w", wantDur: 0, wantErr: true},
		{input: "69y", wantDur: 0, wantErr: true},

		// redundant leading zeros
		{input: "00s", wantDur: 0, wantErr: true},
		{input: "00m", wantDur: 0, wantErr: true},
		{input: "00h", wantDur: 0, wantErr: true},
		{input: "00d", wantDur: 0, wantErr: true},
		{input: "00w", wantDur: 0, wantErr: true},
		{input: "00y", wantDur: 0, wantErr: true},

		// negative values
		{input: "-1s", wantDur: 0, wantErr: true},
		{input: "-1m", wantDur: 0, wantErr: true},
		{input: "-1h", wantDur: 0, wantErr: true},
		{input: "-1d", wantDur: 0, wantErr: true},
		{input: "-1w", wantDur: 0, wantErr: true},
		{input: "-1y", wantDur: 0, wantErr: true},

		// redundant units
		{input: "0ss", wantDur: 0, wantErr: true},
		{input: "0mm", wantDur: 0, wantErr: true},
		{input: "0hh", wantDur: 0, wantErr: true},
		{input: "0dd", wantDur: 0, wantErr: true},
		{input: "0ww", wantDur: 0, wantErr: true},
		{input: "0yy", wantDur: 0, wantErr: true},

		// no unit
		{input: "0", wantDur: 0, wantErr: true},

		// multiple units
		{input: "1h1m", wantDur: 0, wantErr: true},
	}
	for _, tc := range testCases {
		d, err := ParseDuration(tc.input)
		if gotErr := err != nil; gotErr != tc.wantErr {
			t.Errorf("unexpected err for input %q, gotErr=%v, wantErr=%v",
				tc.input, gotErr, tc.wantErr)
		}
		if d != tc.wantDur {
			t.Errorf("duration unmatch for input %q, got=%v, want=%v",
				tc.input, d, tc.wantDur)
		}
	}
}

func TestDuration_String(t *testing.T) {
	testCases := []struct {
		input Duration
		want  string
	}{
		{input: 0, want: "0s"},
		{input: 1, want: "1s"},
		{input: 2, want: "2s"},
		{input: 60, want: "1m"},
		{input: 61, want: "61s"},
		{input: 120, want: "2m"},
		{input: 60 * 60, want: "1h"},
		{input: 2 * 60 * 60, want: "2h"},
		{input: 24 * 60 * 60, want: "1d"},
		{input: 2 * 24 * 60 * 60, want: "2d"},
		{input: 7 * 24 * 60 * 60, want: "1w"},
		{input: 2 * 7 * 24 * 60 * 60, want: "2w"},
		{input: 365 * 24 * 60 * 60, want: "1y"},
		{input: 2 * 365 * 24 * 60 * 60, want: "2y"},
		{input: math.MaxInt32, want: "2147483647s"},
		{input: -1, want: "-1s"},
		{input: math.MinInt32, want: "-2147483648s"},
	}
	for _, tc := range testCases {
		got := tc.input.String()
		if got != tc.want {
			t.Errorf("string unmatch for input %v, got=%s, want=%s",
				tc.input, got, tc.want)
		}
	}
}

func TestParseTimestamp(t *testing.T) {
	testCases := []struct {
		input   string
		wantTs  Timestamp
		wantErr bool
	}{
		{input: "2020-06-20T11:51:23Z", wantTs: 1592653883, wantErr: false},
		{input: "2020-06-20T11:51:23+00:00", wantTs: 0, wantErr: true},
	}
	for _, tc := range testCases {
		ts, err := ParseTimestamp(tc.input)
		if gotErr := err != nil; gotErr != tc.wantErr {
			t.Errorf("unexpected err for input %q, gotErr=%v, wantErr=%v",
				tc.input, gotErr, tc.wantErr)
		}
		if ts != tc.wantTs {
			t.Errorf("timestamp unmatch for input %q, got=%v, want=%v",
				tc.input, ts, tc.wantTs)
		}
	}
}

func TestTimestamp_Truncate(t *testing.T) {
	testCases := []struct {
		t    Timestamp
		d    Duration
		want Timestamp
	}{
		{
			t:    TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 0, 0, time.UTC)),
			d:    5 * Second,
			want: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 0, 0, time.UTC)),
		},
		{
			t:    TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 4, 0, time.UTC)),
			d:    5 * Second,
			want: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 0, 0, time.UTC)),
		},
		{
			t:    TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 5, 0, time.UTC)),
			d:    5 * Second,
			want: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 5, 0, time.UTC)),
		},
		{
			t:    TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 7, 0, time.UTC)),
			d:    5 * Second,
			want: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 5, 0, time.UTC)),
		},
		{
			t:    TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 9, 0, time.UTC)),
			d:    5 * Second,
			want: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 5, 0, time.UTC)),
		},
		{
			t:    TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 10, 0, time.UTC)),
			d:    5 * Second,
			want: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 10, 0, time.UTC)),
		},
		{
			t:    TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 5, 0, time.Local)),
			d:    5 * Second,
			want: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 5, 0, time.Local)),
		},
		{
			t:    TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 6, 0, time.Local)),
			d:    5 * Second,
			want: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 5, 0, time.Local)),
		},
		{
			t:    TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 10, 0, time.Local)),
			d:    5 * Second,
			want: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 10, 0, time.Local)),
		},
		{
			t:    TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 14, 0, time.Local)),
			d:    5 * Second,
			want: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 50, 10, 0, time.Local)),
		},
		{
			t:    TimestampFromStdTime(time.Date(2020, 6, 28, 9, 55, 0, 999999999, time.UTC)),
			d:    0,
			want: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 55, 0, 999999999, time.UTC)),
		},
		{
			t:    TimestampFromStdTime(time.Date(2020, 6, 28, 9, 55, 0, 999999999, time.UTC)),
			d:    -Second,
			want: TimestampFromStdTime(time.Date(2020, 6, 28, 9, 55, 0, 999999999, time.UTC)),
		},
	}
	for _, tc := range testCases {
		got := tc.t.Truncate(tc.d)
		if got != tc.want {
			t.Errorf("timestamp for t=%s, d=%s, got=%s, want=%s", tc.t, tc.d, got, tc.want)
		} else {
			t.Logf("t=%s, d=%s, got=%s", tc.t, tc.d, got)
		}
	}

}
