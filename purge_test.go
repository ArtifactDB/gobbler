package main

import (
    "testing"
    "os"
    "path/filepath"
    "time"
    "errors"
)

func TestPurgeOldFiles (t *testing.T) {
    dir, err := os.MkdirTemp("", "")
    if (err != nil) {
        t.Fatalf("failed to create a temporary directory; %v", err)
    }

    path := filepath.Join(dir, "A")
    err = os.WriteFile(path, []byte(""), 0644)
    if err != nil {
        t.Fatalf("failed to create a mock file; %v", err)
    }

    subdir := filepath.Join(dir, "sub")
    err = os.Mkdir(subdir, 0755)
    if err != nil {
        t.Fatalf("failed to create a temporary subdirectory; %v", err)
    }

    subpath := filepath.Join(subdir, "B")
    err = os.WriteFile(subpath, []byte(""), 0644)
    if err != nil {
        t.Fatalf("failed to create a mock file; %v", err)
    }

    // Also mocking up a symlink to ensure that this is handled sensibly.
    var target string
    {
        handle, err := os.CreateTemp("", "")
        if err != nil {
            t.Fatalf("failed to create a temporary file; %v", err)
        }
        target = handle.Name()
        handle.Close()
    }
    sympath := filepath.Join(dir, "C")
    err = os.Symlink(target, sympath)
    if err != nil {
        t.Fatalf("failed to create a symlink; %v", err)
    }

    // Deleting with a 1-hour expiry.
    err = purgeOldFiles(dir, 1 * time.Hour, nil)
    if (err != nil) {
        t.Fatal(err)
    }
    if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
        t.Error("should not have deleted this file")
    }
    if _, err := os.Stat(subpath); errors.Is(err, os.ErrNotExist) {
        t.Error("should not have deleted this file")
    }
    if _, err := os.Stat(sympath); errors.Is(err, os.ErrNotExist) {
        t.Error("should not have deleted this file")
    }

    // Deleting with an immediate expiry but also protection.
    err = purgeOldFiles(dir, 0 * time.Hour, map[string]bool{ "A": true, "sub": true })
    if (err != nil) {
        t.Fatal(err)
    }
    if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
        t.Error("should not have deleted this file")
    }
    if _, err := os.Stat(subdir); errors.Is(err, os.ErrNotExist) {
        t.Error("should not have deleted this directory")
    }

    if _, err := os.Stat(sympath); !errors.Is(err, os.ErrNotExist) { // Symlink can be deleted, but not its target.
        t.Error("should have deleted the symlink")
    }
    if _, err := os.Stat(target); errors.Is(err, os.ErrNotExist) {
        t.Error("should not have deleted the symlink target")
    }

    // Deleting with an immediate expiry.
    err = purgeOldFiles(dir, 0 * time.Hour, nil)
    if (err != nil) {
        t.Fatal(err)
    }
    if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
        t.Error("should not have deleted the entire directory")
    }
    if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
        t.Error("should have deleted this file")
    }
    if _, err := os.Stat(subdir); !errors.Is(err, os.ErrNotExist) {
        t.Error("should have deleted this directory")
    }
}
