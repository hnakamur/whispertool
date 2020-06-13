package whispertool

import (
	"os"

	"golang.org/x/sys/unix"
)

func pwritev(file *os.File, iovs [][]byte, offset int64) (n int, err error) {
	return unix.Pwritev(int(file.Fd()), iovs, offset)
}

func preadv(file *os.File, iovs [][]byte, offset int64) (n int, err error) {
	return unix.Preadv(int(file.Fd()), iovs, offset)
}
