package whispertool

import "testing"

func TestFloorMod(t *testing.T) {
	testCases := []struct {
		x    int64
		y    int64
		want int64
	}{
		{x: 5, y: 3, want: 2},
		{x: -5, y: 3, want: 1},
		{x: 5, y: -3, want: -1},
		{x: -5, y: -3, want: -2},
		{x: 5, y: 5, want: 0},
		{x: -5, y: 5, want: 0},
		{x: 5, y: -5, want: 0},
		{x: -5, y: -5, want: 0},
		{x: 3, y: 5, want: 3},
		{x: -3, y: 5, want: 2},
		{x: 3, y: -5, want: -2},
		{x: -3, y: -5, want: -3},
	}
	for _, tc := range testCases {
		got := floorMod(tc.x, tc.y)
		if got != tc.want {
			t.Errorf("unexpected floorMod x=%d, y=%d, got=%v, want=%v",
				tc.x, tc.y, got, tc.want)
		}
	}
}
