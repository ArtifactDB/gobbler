package main

import (
    "testing"
    "os"
    "path/filepath"
)

func TestReadLatest(t *testing.T) {
    f, err := os.MkdirTemp("", "test-")
    if err != nil {
        t.Fatalf("failed to create tempdir; %v", err)
    }

    err = os.WriteFile(
        filepath.Join(f, LatestFileName),
        []byte(`{ "latest": "argle" }`),
        0644,
    )
    if err != nil {
        t.Fatalf("failed to create test ..latest; %v", err)
    }

    out, err := ReadLatest(f)
    if err != nil {
        t.Fatalf("failed to read test ..latest; %v", err)
    }

    if out.Latest != "argle" {
        t.Fatalf("unexpected 'latest' value")
    }
}
