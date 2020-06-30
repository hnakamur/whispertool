package whispertool

import (
	"io"
	"os"
	"syscall"
)

type Whisper struct {
	file     *os.File
	fileData *fileData

	inMemory bool
	readOnly bool
	data     []byte
}

type CreateOption func(*Whisper)

func WithInMemory() CreateOption {
	return func(w *Whisper) {
		w.inMemory = true
	}
}

// WithRawData set the raw data for the whisper file.
// If this option is used, retentions, aggregationMethod, and xFilesFactor arguments
// passed to Create will be ignored.
func WithRawData(data []byte) CreateOption {
	return func(w *Whisper) {
		w.data = data
	}
}

type OpenOption func(*Whisper)

func WithReadOnly() OpenOption {
	return func(w *Whisper) {
		w.readOnly = true
	}
}

func Create(filename string, retentions []Retention, aggregationMethod AggregationMethod, xFilesFactor float32, opts ...CreateOption) (*Whisper, error) {
	w := &Whisper{}
	for _, opt := range opts {
		opt(w)
	}

	if !w.inMemory {
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
		if err != nil {
			return nil, err
		}

		if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
			file.Close()
			return nil, err
		}
		w.file = file
	}

	var err error
	if w.data != nil {
		d := &fileData{buf: w.data}
		if err := d.readMetaAndRetentions(); err != nil {
			return nil, err
		}
		d.setPagesDirtyByOffsetRange(0, uint32(len(d.buf)))
		w.fileData = d
	} else {
		w.fileData, err = newFileData(retentions, aggregationMethod, xFilesFactor)
	}
	if err != nil {
		return nil, err
	}
	return w, nil
}

func Open(filename string, opts ...OpenOption) (*Whisper, error) {
	w := &Whisper{}
	for _, opt := range opts {
		opt(w)
	}

	if w.readOnly {
		file, err := os.Open(filename)
		if err != nil {
			return nil, err
		}
		w.file = file
	} else {
		file, err := os.OpenFile(filename, os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

		if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
			file.Close()
			return nil, err
		}
	}

	st, err := w.file.Stat()
	if err != nil {
		return nil, err
	}

	buf := make([]byte, st.Size())
	if _, err := io.ReadFull(w.file, buf); err != nil {
		return nil, err
	}

	fileData, err := newFileDataRead(buf)
	if err != nil {
		return nil, err
	}
	w.fileData = fileData

	return w, nil
}

func (w *Whisper) Sync() error {
	if w.inMemory {
		return nil
	}

	if err := w.fileData.FlushTo(w.file); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	return nil
}

func (w *Whisper) Close() error {
	if w.inMemory {
		return nil
	}

	return w.file.Close()
}

func (w *Whisper) AggregationMethod() AggregationMethod { return w.fileData.meta.aggregationMethod }
func (w *Whisper) XFilesFactor() float32                { return w.fileData.meta.xFilesFactor }
func (w *Whisper) Retentions() []Retention              { return w.fileData.retentions }

// RawData returns data for whole file.
func (w *Whisper) RawData() []byte {
	return w.fileData.buf
}

func (w *Whisper) FetchFromArchive(retentionID int, from, until, now Timestamp) ([]Point, error) {
	return w.fileData.FetchFromArchive(retentionID, from, until, now)
}

func (w *Whisper) GetAllRawUnsortedPoints(retentionID int) []Point {
	return w.fileData.GetAllRawUnsortedPoints(retentionID)
}

func (w *Whisper) UpdatePointForArchive(retentionID int, t Timestamp, v Value, now Timestamp) error {
	return w.fileData.UpdatePointForArchive(retentionID, t, v, now)
}

func (w *Whisper) UpdatePointsForArchive(retentionID int, points []Point, now Timestamp) error {
	return w.fileData.UpdatePointsForArchive(retentionID, points, now)
}

func (w *Whisper) PrintHeader(wr io.Writer) error {
	return w.fileData.PrintHeader(wr)
}
