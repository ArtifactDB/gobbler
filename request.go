package main

import (
    "sync"
    "strings"
    "path/filepath"
    "net/http"
    "time"
    "os"
    "fmt"
    "errors"
    "io/fs"
    "syscall"
)


// This tracks the requests that are currently being processed, to prevent the
// same request being processed multiple times at the same time.
type activeRequestRegistry struct {
    Lock sync.Mutex
    Active map[string]bool
    Expiry time.Duration
}

func newActiveRequestRegistry(staging string, expiry time.Duration) (*activeRequestRegistry, error) {
    output := &activeRequestRegistry {
        Active: map[string]bool{},
        Expiry: expiry,
    }

    // Prefilling the registry ensures that a user can't replay requests after a restart of the service.
    entries, err := os.ReadDir(staging)
    if err != nil {
        return nil, fmt.Errorf("failed to list existing request files in '%s'", staging)
    }

    // Technically we only need to do this for files that weren't already expired, but this doesn't hurt.
    for _, e := range entries {
        path := e.Name()
        output.Add(path)
    }
    return output, nil
}

func (a *activeRequestRegistry) Add(path string) bool {
    a.Lock.Lock()
    defer a.Lock.Unlock()

    _, ok := a.Active[path]
    if ok {
        return false
    }

    a.Active[path] = true

    // Once the request expires, we no longer need to protect against replay attacks,
    // so we can delete it from the registry.
    go func() {
        time.Sleep(a.Expiry)
        a.Lock.Lock()
        defer a.Lock.Unlock()
        delete(a.Active, path)
    }()

    return true
}

func checkRequestFile(path, staging string, expiry time.Duration) (string, error) {
    if !strings.HasPrefix(path, "request-") {
        return "", newHttpError(http.StatusBadRequest, errors.New("file name should start with \"request-\""))
    }

    if path != filepath.Base(path) {
        return "", newHttpError(http.StatusBadRequest, errors.New("path should be the name of a file in the staging directory"))
    }
    reqpath := filepath.Join(staging, path)

    info, err := os.Lstat(reqpath)
    if err != nil {
        return "", newHttpError(http.StatusBadRequest, fmt.Errorf("failed to access path; %v", err))
    }

    if info.IsDir() {
        return "", newHttpError(http.StatusBadRequest, errors.New("path is a directory"))
    }

    if info.Mode() & fs.ModeSymlink != 0 {
        return "", newHttpError(http.StatusBadRequest, errors.New("path is a symbolic link"))
    }

    s, ok := info.Sys().(*syscall.Stat_t)
    if !ok {
        return "", fmt.Errorf("failed to convert to a syscall.Stat_t; %w", err)
    }
    if uint32(s.Nlink) > 1 {
        return "", newHttpError(http.StatusBadRequest, errors.New("path seems to have multiple hard links"))
    }

    current := time.Now() 
    if current.Sub(info.ModTime()) >= expiry {
        return "", newHttpError(http.StatusBadRequest, errors.New("request file is expired"))
    }

    return reqpath, nil
}
