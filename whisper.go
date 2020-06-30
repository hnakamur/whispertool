package whispertool

import (
	"io"
	"os"
	"syscall"
)

// Whisper represents a Whisper database file.
type Whisper struct {
	file     *os.File
	fileData *fileData

	openFileFlag int
	flock        bool
	perm         os.FileMode
	inMemory     bool
	data         []byte
}

// Option is the type for options for creating or opening a whisper file.
type Option func(*Whisper)

// WithInMemory enables to create a whisper database in memory without creating a file.
// This options is useful for only for `Create`.
func WithInMemory() Option {
	return func(w *Whisper) {
		w.inMemory = true
	}
}

// WithFlock enables flock for the file.
// This option is useful only when no WithInMemory is passed.
func WithFlock() Option {
	return func(w *Whisper) {
		w.flock = true
	}
}

// WithRawData set the raw data for the whisper file.
// If this option is used, retentions, aggregationMethod, and xFilesFactor arguments
// passed to Create will be ignored.
// This options is useful for only for Create.
func WithRawData(data []byte) Option {
	return func(w *Whisper) {
		w.data = data
	}
}

// WithOpenFileFlag sets the flag for opening the file.
// This option is useful only when no WithInMemory is passed.
// Without this option, the default value is
// os.O_RDWR | os.O_CREATE | os.O_EXCL for Create and
// os.O_RDWR for Open.
func WithOpenFileFlag(flag int) Option {
	return func(w *Whisper) {
		w.openFileFlag = flag
	}
}

// WithPerm sets the permission for the file.
// This option is useful only when no WithInMemory is passed.
// Without this option, the default value is 0644.
func WithPerm(perm os.FileMode) Option {
	return func(w *Whisper) {
		w.perm = perm
	}
}

// Create creates a whisper database file.
func Create(filename string, retentions []Retention, aggregationMethod AggregationMethod, xFilesFactor float32, opts ...Option) (*Whisper, error) {
	w := &Whisper{
		openFileFlag: os.O_RDWR | os.O_CREATE | os.O_EXCL,
		perm:         0644,
	}
	for _, opt := range opts {
		opt(w)
	}

	if !w.inMemory {
		err := w.openAndLockFile(filename)
		if err != nil {
			return nil, err
		}
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

// Open opens an existing whisper database file.
// In the current implementation, the whole content is read after opening the file.
// This behavior may be changed in the future.
func Open(filename string, opts ...Option) (*Whisper, error) {
	w := &Whisper{
		openFileFlag: os.O_RDWR,
		perm:         0644,
	}
	for _, opt := range opts {
		opt(w)
	}

	err := w.openAndLockFile(filename)
	if err != nil {
		return nil, err
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

func (w *Whisper) openAndLockFile(filename string) error {
	file, err := os.OpenFile(filename, w.openFileFlag, w.perm)
	if err != nil {
		return err
	}
	w.file = file

	if w.flock {
		if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
			file.Close()
			return err
		}
	}
	return nil
}

// Sync flushes modifications on the memory buffer to the file and
// sync commits the content to the storage by calling os.File.Sync().
// Note it is caller's responsibility to call Sync and the modification
// will be lost without calling Sync.
// For the file created with WithInMemory, this is a no-op.
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

// Close closes the file.
// For the file created with WithInMemory, this is a no-op.
func (w *Whisper) Close() error {
	if w.inMemory {
		return nil
	}

	return w.file.Close()
}

// AggregationMethod returns the aggregation method of the whisper file.
func (w *Whisper) AggregationMethod() AggregationMethod { return w.fileData.meta.aggregationMethod }

// XFilesFactor returns the xFilesFactor of the whisper file.
func (w *Whisper) XFilesFactor() float32 { return w.fileData.meta.xFilesFactor }

// Retentions returns the retentions of the whisper file.
func (w *Whisper) Retentions() Retentions { return w.fileData.retentions }

// RawData returns data for whole file.
// Note the byte slice returned is the internal work buffer,
// without cloning in favor of performance.
// It is caller's responsibility to not modify the data.
func (w *Whisper) RawData() []byte {
	return w.fileData.buf
}

// FetchFromArchive fetches point in the specified archive and the time range.
// If now is zero, the current time is used.
func (w *Whisper) FetchFromArchive(retentionID int, from, until, now Timestamp) ([]Point, error) {
	return w.fileData.FetchFromArchive(retentionID, from, until, now)
}

// GetAllRawUnsortedPoints returns the raw unsorted points.
// This is provided for the debugging or investination purpose.
func (w *Whisper) GetAllRawUnsortedPoints(retentionID int) []Point {
	return w.fileData.GetAllRawUnsortedPoints(retentionID)
}

// UpdatePointForArchive updates one point in the specified archive.
func (w *Whisper) UpdatePointForArchive(retentionID int, t Timestamp, v Value, now Timestamp) error {
	return w.fileData.UpdatePointForArchive(retentionID, t, v, now)
}

// UpdatePointForArchive updates points in the specified archive.
func (w *Whisper) UpdatePointsForArchive(retentionID int, points []Point, now Timestamp) error {
	return w.fileData.UpdatePointsForArchive(retentionID, points, now)
}

// PrintHeader prints the header information to the writer in LTSV format [1].
// [1] Labeled Tab-separated Values http://ltsv.org/
func (w *Whisper) PrintHeader(wr io.Writer) error {
	return w.fileData.PrintHeader(wr)
}
