package main

import (
    "testing"
    "os"
    "path/filepath"
    "sort"
)

func TestListFiles(t *testing.T) {
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

    // Checking that we pull out all the files.
    all, err := listFiles(dir, true)
    if (err != nil) {
        t.Fatal(err)
    }

    sort.Strings(all)
    if len(all) != 2 || all[0] != "A" || all[1] != "sub/B" {
        t.Errorf("unexpected results from the listing (%q)", all)
    }

    // Checking that the directories are properly listed.
    all, err = listFiles(dir, false)
    if (err != nil) {
        t.Fatal(err)
    }

    sort.Strings(all)
    if len(all) != 2 || all[0] != "A" || all[1] != "sub/" {
        t.Errorf("unexpected results from the listing (%q)", all)
    }
}
