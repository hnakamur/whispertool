package whispertool

import (
	"reflect"
	"testing"

	"github.com/willf/bitset"
)

func TestDirtyPageRanges(t *testing.T) {
	b := bitset.New(9)
	b.Set(0).Set(1).Set(3).Set(6).Set(7).Set(8)
	got := dirtyPageRanges(b)
	want := []pageRange{
		{start: 0, end: 2},
		{start: 3, end: 4},
		{start: 6, end: 9},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ranges unmatch, got=%+v, want=%+v", got, want)
	}
}
