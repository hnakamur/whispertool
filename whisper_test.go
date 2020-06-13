package whispertool

import (
	"os"
	"testing"
)

func TestWhisper(t *testing.T) {
	w, err := Open("sv01.wsp", NewBufferPool(os.Getpagesize()))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err = w.readPagesIfNeeded(7, 8); err != nil {
		t.Fatal(err)
	}

	if err = w.readPagesIfNeeded(3, 3); err != nil {
		t.Fatal(err)
	}

	if err = w.readPagesIfNeeded(2, 5); err != nil {
		t.Fatal(err)
	}
}
