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

func chooseLockPool(path string, num_pools int) int {
    sum := 0
    for _, r := range path {
        sum += int(r)
    }
    return sum % num_pools
}

// This tracks the requests that are currently being processed, to prevent the
// same request being processed multiple times at the same time. We use a
// multi-pool approach to improve parallelism across requests.
type activeRequestRegistry struct {
    NumPools int
    Locks []sync.Mutex
    Active []map[string]bool
}

func newActiveRequestRegistry(num_pools int) *activeRequestRegistry {
    return &activeRequestRegistry {
        NumPools: num_pools,
        Locks: make([]sync.Mutex, num_pools),
        Active: make([]map[string]bool, num_pools),
    }
}

func prefillActiveRequestRegistry(a *activeRequestRegistry, staging string, expiry time.Duration) error {
    // Prefilling the registry ensures that a user can't replay requests after a restart of the service.
    entries, err := os.ReadDir(staging)
    if err != nil {
        return fmt.Errorf("failed to list existing request files in '%s'", staging)
    }

    // This is only necessary until the expiry time is exceeded, after which we can evict those entries.
    // Technically we only need to do this for files that weren't already expired, but this doesn't hurt.
    for _, e := range entries {
        path := e.Name()
        a.Add(path)
        go func(p string) {
            time.Sleep(expiry)
            a.Remove(p)
        }(path)
    }
    return nil
}

func (a *activeRequestRegistry) Add(path string) bool {
    i := chooseLockPool(path, a.NumPools)
    a.Locks[i].Lock()
    defer a.Locks[i].Unlock()

    if a.Active[i] == nil {
        a.Active[i] = map[string]bool{}
    } else {
        _, ok := a.Active[i][path]
        if ok {
            return false
        }
    }
   
    a.Active[i][path] = true
    return true
}

func (a *activeRequestRegistry) Remove(path string) {
    i := chooseLockPool(path, a.NumPools)
    a.Locks[i].Lock()
    defer a.Locks[i].Unlock()
    delete(a.Active[i], path)
}

func checkRequestFile(path, staging string, expiry time.Duration) (string, error) {
    if !strings.HasPrefix(path, "request-") {
        return "", newHttpError(http.StatusBadRequest, errors.New("file name should start with \"request-\""))
    }

    if !filepath.IsLocal(path) {
        return "", newHttpError(http.StatusBadRequest, errors.New("path should be local to the staging directory"))
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
