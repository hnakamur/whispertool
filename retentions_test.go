package whispertool

import "testing"

func TestParseRetentions(t *testing.T) {
	testCases := []struct {
		input   string
		want    Retentions
		wantErr bool
	}{
		{
			input: "1m:2h",
			want: []Retention{
				NewRetention(Minute, 120),
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
