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
)

func listFiles(dir string, recursive bool) ([]string, error) {
    to_report := []string{}

    err := filepath.WalkDir(dir, func(path string, info fs.DirEntry, err error) error {
        if err != nil {
            return err
        }

        is_dir := info.IsDir()
        if is_dir {
            if recursive || dir == path {
                return nil
            }
        }

        rel, err := filepath.Rel(dir, path)
        if err != nil {
            return err
        }

        if !recursive && is_dir {
            to_report = append(to_report, rel + "/")
            return fs.SkipDir
        } else {
            to_report = append(to_report, rel)
            return nil
        }
    })

    if err != nil {
        return nil, fmt.Errorf("failed to obtain a directory listing; %w", err)
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

    all, err := listFiles(path, recursive)
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
