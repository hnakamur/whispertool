package whispertool

import (
	"sort"
	"testing"
)

func TestParseRetention(t *testing.T) {
	testCases := []struct {
		input         string
		wantPrecision Duration
		wantNrPts     uint32
		wantErr       bool
	}{
		{input: "1s:5m", wantPrecision: Second, wantNrPts: 300, wantErr: false},
		{input: "1m:30m", wantPrecision: Minute, wantNrPts: 30, wantErr: false},
		{input: "1m", wantPrecision: 0, wantNrPts: 0, wantErr: true},
		{input: "1m:30m:20s", wantPrecision: 0, wantNrPts: 0, wantErr: true},
		{input: "1f:30s", wantPrecision: 0, wantNrPts: 0, wantErr: true},
		{input: "1m:30f", wantPrecision: 0, wantNrPts: 0, wantErr: true},
	}
	for _, tc := range testCases {
		r, err := ParseRetention(tc.input)
		if gotErr := err != nil; gotErr != tc.wantErr {
			t.Errorf("unexpected err for input %q, gotErr=%v, wantErr=%v",
				tc.input, gotErr, tc.wantErr)
		}
		if err == nil {
			if got, want := r.SecondsPerPoint(), tc.wantPrecision; got != want {
				t.Errorf("retention precision unmatch for input %q, got=%s, want=%s",
					tc.input, got, want)
			}
			if got, want := r.NumberOfPoints(), tc.wantNrPts; got != want {
				t.Errorf("retention number of points unmatch for input %q, got=%d, want=%d",
					tc.input, got, want)
			}
		}
	}
}

func TestParseRetentions(t *testing.T) {
	testCases := []struct {
		input   string
		want    Retentions
		wantErr bool
	}{
		{
			input: "1s:5m",
			want: []Retention{
				NewRetention(Second, 300),
			},
			wantErr: false,
		},
		{
			input: "1m:2h,1h:2d",
			want: []Retention{
				NewRetention(Minute, 120),
				NewRetention(Hour, 48),
			},
			wantErr: false,
		},
		{
			input: "1m:2h,1h:2d,1d:32d",
			want: []Retention{
				NewRetention(Minute, 120),
				NewRetention(Hour, 48),
				NewRetention(Day, 32),
			},
			wantErr: false,
		},
		{input: "3m:5m", want: nil, wantErr: true},
		{input: "1h:1m", want: nil, wantErr: true},
		{input: "1m:30m:20s", want: nil, wantErr: true},
		{input: "", want: nil, wantErr: true},
	}
	for _, tc := range testCases {
		rr, err := ParseRetentions(tc.input)
		if gotErr := err != nil; gotErr != tc.wantErr {
			t.Errorf("unexpected err for input %q, gotErr=%v, wantErr=%v",
				tc.input, gotErr, tc.wantErr)
		}

		gotStr := rr.String()
		wantStr := tc.want.String()
		if gotStr != wantStr {
			t.Errorf("retentions unmatch for input %q, got=%s, want=%s",
				tc.input, gotStr, wantStr)
		}
	}
}

func TestSortRetentions(t *testing.T) {
	retentions := Retentions{
		{secondsPerPoint: 300, numberOfPoints: 12},
		{secondsPerPoint: 60, numberOfPoints: 30},
		{secondsPerPoint: 1, numberOfPoints: 300},
	}
	sort.Sort(retentionsByPrecision(retentions))
	if retentions[0].secondsPerPoint != 1 {
		t.Fatalf("Retentions array is not sorted")
	}
}
