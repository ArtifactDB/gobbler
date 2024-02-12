package main

import (
    "os"
    "path/filepath"
    "time"
    "strings"
    "errors"
)

func purgeOldFiles(dir string, limit time.Duration) error {
    var to_delete []string
    present := time.Now()
    messages := []string{}

    filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
        if (err != nil) {
            messages = append(messages, "failed to walk into '" + path + "'; " + err.Error())
            return nil
        }
        if (path == dir) {
            return nil
        }

        delta := present.Sub(info.ModTime())
        if (delta > limit) {
            to_delete = append(to_delete, path)
        }

        if (info.IsDir()) {
            return filepath.SkipDir
        }

        return nil
    })

    for _, x := range to_delete {
        err := os.RemoveAll(x)
        if (err != nil) {
            messages = append(messages, "failed to delete '" + x + "'; " + err.Error())
        }
    }

    if len(messages) > 0 {
        return errors.New(strings.Join(messages, "; "))
    }

    return nil
}
