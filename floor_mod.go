package whispertool

// Go (truncated division)
//  x     y     x / y     x % y
//  5     3       1         2
// -5     3      -1        -2
//  5    -3      -1         2
// -5    -3       1        -2
//
// Python3 (floored division)
//  x     y     x // y     x % y
//  5     3        1         2
// -5     3       -2         1
//  5    -3       -2        -1
// -5    -3        1        -2
func floorMod(x, y int64) int64 {
	m := x % y
	if m == 0 || ((x >= 0 && y > 0) || (x < 0 && y < 0)) {
		return m
	}
	return m + y
}
