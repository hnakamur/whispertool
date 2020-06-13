package whispertool

import (
	"log"
	"testing"
)

func TestWhisper(t *testing.T) {
	w, err := Open("src.wsp")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	log.Printf("w=%+v", w)
}
