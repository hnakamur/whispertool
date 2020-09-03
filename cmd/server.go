package cmd

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hnakamur/whispertool"
)

const RespHeaderNameXOp = "X-Op"
const RespHeaderNameXPath = "X-Path"

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
	http.HandleFunc("/view-raw", wrapHandler(a.handleViewRaw))
	http.HandleFunc("/sum", wrapHandler(a.handleSum))
	http.HandleFunc("/items", wrapHandler(a.handleItems))
	http.HandleFunc("/files", wrapHandler(a.handleFiles))
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

func (a *app) handleViewRaw(w http.ResponseWriter, r *http.Request) error {
	if err := r.ParseForm(); err != nil {
		return newHTTPError(http.StatusBadRequest, errors.New("cannot parse form"))
	}
	retID, err := getFormInt(r, "retention")
	if err != nil {
		return err
	}
	fileParam := r.Form.Get("file")
	if fileParam == "" {
		return newHTTPError(http.StatusBadRequest, errors.New("\"file\" parameter must not be empty"))
	}

	filename := filepath.Join(a.baseDir, fileParam)
	h, ptsList, err := readWhisperFileRawLocal(filename, retID)
	if err != nil {
		return err
	}

	var buf []byte
	buf = h.AppendTo(buf)
	for i := range h.ArchiveInfoList() {
		buf = ptsList[i].AppendTo(buf)
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = w.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func (a *app) handleView(w http.ResponseWriter, r *http.Request) error {
	if err := r.ParseForm(); err != nil {
		return newHTTPError(http.StatusBadRequest, errors.New("cannot parse form"))
	}
	retID, err := getFormInt(r, "retention")
	if err != nil {
		return err
	}
	fileParam := r.Form.Get("file")
	if fileParam == "" {
		return newHTTPError(http.StatusBadRequest, errors.New("\"file\" parameter must not be empty"))
	}
	from, err := whispertool.ParseTimestamp(r.Form.Get("from"))
	if err != nil {
		return newHTTPError(http.StatusBadRequest, errors.New("cannot parse \"from\" parameter"))
	}
	until, err := whispertool.ParseTimestamp(r.Form.Get("until"))
	if err != nil {
		return newHTTPError(http.StatusBadRequest, errors.New("cannot parse \"until\" parameter"))
	}
	now, err := whispertool.ParseTimestamp(r.Form.Get("now"))
	if err != nil {
		return newHTTPError(http.StatusBadRequest, errors.New("cannot parse \"now\" parameter"))
	}

	filename := filepath.Join(a.baseDir, fileParam)
	h, tsList, err := readWhisperFileLocal(filename, retID, from, until, now)
	if err != nil {
		if os.IsNotExist(err) {
			return setRespForNotExistErr(w, err)
		}
		return err
	}

	var buf []byte
	buf = h.AppendTo(buf)
	for i := range h.ArchiveInfoList() {
		buf = tsList[i].AppendTo(buf)
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = w.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func setRespForNotExistErr(w http.ResponseWriter, err error) error {
	w.Header().Set("Content-Type", "application/octet-stream")
	var perr *os.PathError
	if errors.As(err, &perr) {
		w.Header().Set(RespHeaderNameXOp, perr.Op)
		w.Header().Set(RespHeaderNameXPath, perr.Path)
	}
	return nil
}

func (a *app) handleSum(w http.ResponseWriter, r *http.Request) error {
	if err := r.ParseForm(); err != nil {
		return newHTTPError(http.StatusBadRequest, errors.New("cannot parse form"))
	}
	item := r.Form.Get("item")
	if item == "" {
		return newHTTPError(http.StatusBadRequest, errors.New("\"item\" parameter must not be empty"))
	}
	pattern := r.Form.Get("pattern")
	if pattern == "" {
		return newHTTPError(http.StatusBadRequest, errors.New("\"pattern\" parameter must not be empty"))
	}
	retID, err := getFormInt(r, "retention")
	if err != nil {
		return err
	}
	from, err := whispertool.ParseTimestamp(r.Form.Get("from"))
	if err != nil {
		return newHTTPError(http.StatusBadRequest, errors.New("cannot parse \"from\" parameter"))
	}
	until, err := whispertool.ParseTimestamp(r.Form.Get("until"))
	if err != nil {
		return newHTTPError(http.StatusBadRequest, errors.New("cannot parse \"until\" parameter"))
	}
	now, err := whispertool.ParseTimestamp(r.Form.Get("now"))
	if err != nil {
		return newHTTPError(http.StatusBadRequest, errors.New("cannot parse \"now\" parameter"))
	}

	h, tsList, err := sumWhisperFileLocal(a.baseDir, item, pattern, retID, from, until, now)
	if err != nil {
		if os.IsNotExist(err) {
			return setRespForNotExistErr(w, err)
		}
		return err
	}

	var buf []byte
	buf = h.AppendTo(buf)
	for i := range h.ArchiveInfoList() {
		buf = tsList[i].AppendTo(buf)
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = w.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func (a *app) handleItems(w http.ResponseWriter, r *http.Request) error {
	if err := r.ParseForm(); err != nil {
		return newHTTPError(http.StatusBadRequest, errors.New("cannot parse form"))
	}
	pattern := r.Form.Get("pattern")
	if pattern == "" {
		return newHTTPError(http.StatusBadRequest, errors.New("\"pattern\" parameter must not be empty"))
	}

	items, err := globItemsLocal(a.baseDir, pattern)
	if err != nil {
		if os.IsNotExist(err) {
			return setRespForNotExistErr(w, err)
		}
		return newHTTPError(http.StatusBadRequest, err)
	}

	w.Header().Set("Content-Type", "text/plain")
	for _, item := range items {
		if _, err := fmt.Fprintf(w, "%s\n", item); err != nil {
			return newHTTPError(http.StatusInternalServerError,
				fmt.Errorf("cannot write item name: %s", err))
		}
	}
	return nil
}

func (a *app) handleFiles(w http.ResponseWriter, r *http.Request) error {
	if err := r.ParseForm(); err != nil {
		return newHTTPError(http.StatusBadRequest, errors.New("cannot parse form"))
	}
	pattern := r.Form.Get("pattern")
	if pattern == "" {
		return newHTTPError(http.StatusBadRequest, errors.New("\"pattern\" parameter must not be empty"))
	}

	items, err := globFilesLocal(a.baseDir, pattern)
	if err != nil {
		if os.IsNotExist(err) {
			return setRespForNotExistErr(w, err)
		}
		return newHTTPError(http.StatusBadRequest, err)
	}

	w.Header().Set("Content-Type", "text/plain")
	for _, item := range items {
		if _, err := fmt.Fprintf(w, "%s\n", item); err != nil {
			return newHTTPError(http.StatusInternalServerError,
				fmt.Errorf("cannot write item name: %s", err))
		}
	}
	return nil
}

func getFormInt(r *http.Request, paramName string) (int, error) {
	strValue := r.Form.Get(paramName)
	if strValue == "" {
		return 0, newHTTPError(http.StatusBadRequest,
			fmt.Errorf("%q parameter must not be empty", paramName))
	}
	value, err := strconv.Atoi(strValue)
	if err != nil {
		return 0, newHTTPError(http.StatusBadRequest,
			fmt.Errorf("invalid integer %s for %q parameter", strValue, paramName))
	}
	return value, nil
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
