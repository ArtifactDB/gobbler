package main

import (
    "testing"
    "os"
    "io/ioutil"
    "path/filepath"
    "time"
)

func TestPurgeOldFiles (t *testing.T) {
    dir, err := ioutil.TempDir("", "")
    if (err != nil) {
        t.Fatal(err)
        return
    }

    path := filepath.Join(dir, "A")
    {
        handle, _ := os.Create(path)
        handle.Close()
    }

    subdir := filepath.Join(dir, "sub")
    os.Mkdir(subdir, 0755)

    subpath := filepath.Join(subdir, "B")
    {
        handle, _ := os.Create(subpath)
        handle.Close()
    }

    // Deleting with a 1-hour expiry.
    err = PurgeOldFiles(dir, 1 * time.Hour)
    if (err != nil) {
        t.Fatal(err)
    }

    if _, ok := os.Stat(path); os.IsNotExist(ok) {
        t.Error("should not have deleted this file")
    }
    if _, ok := os.Stat(subpath); os.IsNotExist(ok) {
        t.Error("should not have deleted this file")
    }

    // Deleting with an immediate expiry.
    err = PurgeOldFiles(dir, 0 * time.Hour)
    if (err != nil) {
        t.Fatal(err)
    }

    if _, ok := os.Stat(dir); os.IsNotExist(ok) {
        t.Error("should not have deleted the entire directory")
    }
    if _, ok := os.Stat(path); !os.IsNotExist(ok) {
        t.Error("should have deleted this file")
    }
    if _, ok := os.Stat(subdir); !os.IsNotExist(ok) {
        t.Error("should have deleted this directory")
    }
}
