package whispertool

import (
	"testing"
)

func TestWhisper(t *testing.T) {
	//	w, err := Open("sv01.wsp", NewBufferPool(os.Getpagesize()))
	//	if err != nil {
	//		t.Fatal(err)
	//	}
	//	defer w.Close()
	//
	//	if err = w.readPagesIfNeeded(7, 8); err != nil {
	//		t.Fatal(err)
	//	}
	//
	//	if err = w.readPagesIfNeeded(3, 3); err != nil {
	//		t.Fatal(err)
	//	}
	//
	//	if err = w.readPagesIfNeeded(2, 5); err != nil {
	//		t.Fatal(err)
	//	}
}

func TestParseRetentions(t *testing.T) {
	//	testCases := []struct {
	//		input   string
	//		want    []Retention
	//		wantErr bool
	//	}{
	//		{
	//			input: "1m:2h",
	//			want: []Retention{
	//				{SecondsPerPoint: Minute, NumberOfPoints: 120},
	//			},
	//			wantErr: false,
	//		},
	//		{
	//			input: "1m:2h,1h:2d",
	//			want: []Retention{
	//				{SecondsPerPoint: Minute, NumberOfPoints: 120},
	//				{SecondsPerPoint: Hour, NumberOfPoints: 48},
	//			},
	//			wantErr: false,
	//		},
	//		{
	//			input: "1m:2h,1h:2d,1d:32d",
	//			want: []Retention{
	//				{SecondsPerPoint: Minute, NumberOfPoints: 120},
	//				{SecondsPerPoint: Hour, NumberOfPoints: 48},
	//				{SecondsPerPoint: Day, NumberOfPoints: 32},
	//			},
	//			wantErr: false,
	//		},
	//		{input: "3m:5m", want: nil, wantErr: true},
	//		{input: "1h:1m", want: nil, wantErr: true},
	//		{input: "", want: nil, wantErr: true},
	//	}
	//	for _, tc := range testCases {
	//		rr, err := ParseRetentions(tc.input)
	//		if gotErr := err != nil; gotErr != tc.wantErr {
	//			t.Errorf("unexpected err for input %q, gotErr=%v, wantErr=%v",
	//				tc.input, gotErr, tc.wantErr)
	//		}
	//
	//		gotStr := Retentions(rr).String()
	//		wantStr := Retentions(tc.want).String()
	//		if gotStr != wantStr {
	//			t.Errorf("retentions unmatch for input %q, got=%s, want=%s",
	//				tc.input, gotStr, wantStr)
	//		}
	//	}
}
