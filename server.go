package whispertool

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"time"
)

type ServerCommand struct {
	Addr    string
	BaseDir string
}

type app struct {
	baseDir string
}

type httpError struct {
	statusText string
	statusCode int
	cause      error
}

func (c *ServerCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.Addr, "addr", ":8080", "listen address")
	fs.StringVar(&c.BaseDir, "base", ".", "base directory")
	fs.Parse(args)

	return nil
}

func (c *ServerCommand) Execute() error {
	a := &app{
		baseDir: c.BaseDir,
	}
	http.HandleFunc("/view", wrapHandler(a.handleView))
	http.HandleFunc("/sum", wrapHandler(a.handleSum))
	s := &http.Server{
		Addr:           c.Addr,
		Handler:        nil,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	return s.ListenAndServe()
}

func wrapHandler(h func(w http.ResponseWriter, r *http.Request) error) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := h(w, r); err != nil {
			log.Printf("error returned from handler: %v", err)
			var hErr *httpError
			if !errors.As(err, &hErr) {
				hErr = newHTTPError(http.StatusInternalServerError, err)
			}
			hErr.WriteTo(w)
		}
	}
}

func (a *app) handleView(w http.ResponseWriter, r *http.Request) error {
	if err := r.ParseForm(); err != nil {
		return newHTTPError(http.StatusBadRequest, errors.New("cannot parse form"))
	}
	now, err := time.Parse(UTCTimeLayout, r.Form.Get("now"))
	if err != nil {
		return newHTTPError(http.StatusBadRequest, errors.New("cannot parse \"now\" parameter"))
	}
	fileParam := r.Form.Get("file")
	if fileParam == "" {
		return newHTTPError(http.StatusBadRequest, errors.New("\"file\" parameter must not be empty"))
	}
	filename := filepath.Join(a.baseDir, fileParam)

	from := time.Unix(0, 0)
	until := now
	tsNow := TimestampFromStdTime(now)
	tsFrom := TimestampFromStdTime(from)
	tsUntil := TimestampFromStdTime(until)

	d, _, err := readWhisperFile(filename, tsNow, tsFrom, tsUntil, RetIDAll)
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = w.Write(d.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (a *app) handleSum(w http.ResponseWriter, r *http.Request) error {
	if err := r.ParseForm(); err != nil {
		return newHTTPError(http.StatusBadRequest, errors.New("cannot parse form"))
	}
	now, err := time.Parse(UTCTimeLayout, r.Form.Get("now"))
	if err != nil {
		return newHTTPError(http.StatusBadRequest, errors.New("cannot parse \"now\" parameter"))
	}
	pattern := r.Form.Get("pattern")
	if pattern == "" {
		return newHTTPError(http.StatusBadRequest, errors.New("\"pattern\" parameter must not be empty"))
	}
	srcFilenames, err := filepath.Glob(filepath.Join(a.baseDir, pattern))
	if err != nil {
		return newHTTPError(http.StatusBadRequest, err)
	}
	if len(srcFilenames) == 0 {
		return newHTTPError(http.StatusBadRequest,
			fmt.Errorf("no file matched for pattern=%s", pattern))
	}

	from := time.Unix(0, 0)
	until := now
	tsNow := TimestampFromStdTime(now)
	tsFrom := TimestampFromStdTime(from)
	tsUntil := TimestampFromStdTime(until)

	sumData, ptsList, err := sumWhisperFile(srcFilenames, tsNow, tsFrom, tsUntil, RetIDAll)
	if err != nil {
		return err
	}

	if err := updateFileDataWithPointsList(sumData, ptsList, tsNow); err != nil {
		return err
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = w.Write(sumData.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func newHTTPError(statusCode int, err error) *httpError {
	return &httpError{
		statusText: http.StatusText(statusCode),
		statusCode: statusCode,
		cause:      err,
	}
}

func (e *httpError) Error() string {
	if e.cause == nil {
		return e.statusText
	}
	return fmt.Sprintf("%s: %s", e.statusText, e.cause)
}

func (e *httpError) Unwrap() error {
	return e.cause
}

func (e *httpError) WriteTo(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain")
	http.Error(w, e.Error(), e.statusCode)
}
