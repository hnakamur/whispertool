// +build !linux

package whispertool

import "os"

func pwritev(file *os.File, iovs [][]byte, offset int64) (n int, err error) {
	var n0 int
	for len(iovs) > 0 {
		n0, err = file.WriteAt(iovs[0], offset)
		n += n0
		if err != nil {
			return n, err
		}
		iovs = iovs[1:]
		offset += int64(n0)
	}
	return n, nil
}

func preadv(file *os.File, iovs [][]byte, offset int64) (n int, err error) {
	var n0 int
	for len(iovs) > 0 {
		n0, err = file.ReadAt(iovs[0], offset)
		n += n0
		if err != nil {
			return n, err
		}
		iovs = iovs[1:]
		offset += int64(n0)
	}
	return n, nil
}
