package cmd

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func isBaseURL(baseDirOrURL string) bool {
	return strings.HasPrefix(baseDirOrURL, "http://") || strings.HasPrefix(baseDirOrURL, "https://")
}

func globItems(baseDirOrURL, itemDirPattern string) ([]string, error) {
	if isBaseURL(baseDirOrURL) {
		return globItemsRemote(baseDirOrURL, itemDirPattern)
	}
	return globItemsLocal(baseDirOrURL, itemDirPattern)
}

func globItemsLocal(baseDir, itemDirPattern string) ([]string, error) {
	itemDirAbsPattern := filepath.Join(baseDir, itemDirPattern)
	items, err := filepath.Glob(itemDirAbsPattern)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, &os.PathError{
			Op:   "glob",
			Path: itemDirAbsPattern,
			Err:  os.ErrNotExist,
		}
	}
	for i, itemDirname := range items {
		itemRelDir, err := filepath.Rel(baseDir, itemDirname)
		if err != nil {
			return nil, err
		}
		items[i] = relDirToItem(itemRelDir)
	}
	return items, nil
}

func globItemsRemote(srcURL, itemRelDirPattern string) ([]string, error) {
	reqURL := fmt.Sprintf("%s/items?pattern=%s",
		srcURL,
		url.QueryEscape(itemRelDirPattern))
	resp, err := http.Get(reqURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// We must always read to the end.
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, convertRemoteErrNotExist(resp)
	}

	var items []string
	s := bufio.NewScanner(bytes.NewBuffer(data))
	for s.Scan() {
		items = append(items, s.Text())
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
