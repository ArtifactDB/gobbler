package main

import (
    "os"
    "path/filepath"
    "fmt"
    "io/fs"
    "errors"
    "strings"
    "net/url"
    "net/http"
    "context"
)

func updateEmptyDirectories(dir string, empty_directories map[string]bool) {
    for {
        dir = filepath.Dir(dir)
        if dir == "." {
            return
        }
        if found, ok := empty_directories[dir]; ok {
            if !found {
                return // some previous iteration already figured out it's not empty, no need to continue onto the parents.
            }
            empty_directories[dir] = false // i.e., it's not empty anymore.
        }
    }
}

func listFiles(dir string, recursive bool, ctx context.Context) ([]string, error) {
    to_report := []string{}
    empty_directories := map[string]bool{}

    err := filepath.WalkDir(dir, func(path string, info fs.DirEntry, err error) error {
        if err != nil {
            return err
        }

        err = ctx.Err()
        if err != nil {
            return fmt.Errorf("list request cancelled; %w", err)
        }

        if dir == path {
            return nil
        }

        rel, err := filepath.Rel(dir, path)
        if err != nil {
            return err
        }

        if info.IsDir() {
            if recursive {
                empty_directories[rel] = true
                return nil
            } else {
                to_report = append(to_report, rel + "/")
                return fs.SkipDir
            }
        } else {
            to_report = append(to_report, rel)
            return nil
        }
    })

    if err != nil {
        return nil, fmt.Errorf("failed to obtain a directory listing; %w", err)
    }

    if recursive {
        for _, fpath := range to_report {
            updateEmptyDirectories(fpath, empty_directories)
        }
        for dpath, empty := range empty_directories {
            if empty {
                updateEmptyDirectories(dpath, empty_directories) // strip out directories that contain other (empty) directories
            }
        }
        for dpath, empty := range empty_directories {
            if empty {
                to_report = append(to_report, dpath + "/")
            }
        }
    }

    return to_report, nil
}

func listFilesHandler(r *http.Request, registry string) ([]string, error) {
    qparams := r.URL.Query()
    path := qparams.Get("path")
    recursive := (qparams.Get("recursive") == "true")

    if path == "" {
        path = registry
    } else {
        var err error
        path, err = url.QueryUnescape(path)
        if err != nil {
            return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'path'; %w", err))
        } else if !filepath.IsLocal(path) {
            return nil, newHttpError(http.StatusBadRequest, errors.New("'path' is not local to the registry"))
        }
        path = filepath.Join(registry, path)
    }

    all, err := listFiles(path, recursive, r.Context())
    return all, err
}

// This refers to non-internal directories that were created by users, e.g., not ..logs.
func listUserDirectories(dir string) ([]string, error) {
    listing, err := os.ReadDir(dir)
    if err != nil {
        return nil, err
    }

    output := []string{}
    for _, entry := range listing {
        if !entry.IsDir() {
            continue
        }
        name := entry.Name()
        if strings.HasPrefix(name, "..") {
            continue
        }
        output = append(output, name)
    }

    return output, nil
}
