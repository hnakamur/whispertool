package whispertool

import (
	"io"
	"io/ioutil"
	"os"
	"syscall"
)

type Whisper struct {
	file     *os.File
	fileData *FileData
}

func (w *Whisper) FileData() *FileData { return w.fileData }

func WriteFile(filename string, fileData *FileData, perm os.FileMode) error {
	w, err := Create(filename, fileData, perm)
	if err != nil {
		return err
	}
	defer w.Close()

	if err := w.Sync(); err != nil {
		return err
	}
	return nil
}

func Create(filename string, fileData *FileData, perm os.FileMode) (*Whisper, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return nil, err
	}

	if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		file.Close()
		return nil, err
	}

	return &Whisper{
		file:     file,
		fileData: fileData,
	}, nil
}

func OpenForWrite(filename string) (*Whisper, error) {
	file, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		file.Close()
		return nil, err
	}

	st, err := file.Stat()
	if err != nil {
		return nil, err
	}

	buf := make([]byte, st.Size())
	if _, err := io.ReadFull(file, buf); err != nil {
		return nil, err
	}

	d, err := NewFileDataRead(buf)
	if err != nil {
		return nil, err
	}

	w := &Whisper{
		file:     file,
		fileData: d,
	}
	return w, nil
}

func ReadFile(filename string) (*FileData, error) {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	d, err := NewFileDataRead(buf)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func (w *Whisper) Sync() error {
	if err := w.fileData.FlushTo(w.file); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	return nil
}

func (w *Whisper) Close() error {
	return w.file.Close()
}
