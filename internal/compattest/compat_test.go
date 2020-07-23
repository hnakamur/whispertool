package compattest

import (
	"os"
	"testing"

	"github.com/go-graphite/go-whisper"
	"github.com/hnakamur/whispertool"
)

var clock = &fixedClock{}

func TestMain(m *testing.M) {
	whispertool.Now = clock.Now
	whisper.Now = clock.Now
	os.Exit(m.Run())
}
