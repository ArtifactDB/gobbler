package main

import (
    "testing"
    "os"
    "path/filepath"
)

func TestReadUsage(t *testing.T) {
    f, err := os.MkdirTemp("", "test-")
    if err != nil {
        t.Fatalf("failed to create tempdir; %v", err)
    }

    err = os.WriteFile(
        filepath.Join(f, UsageFileName),
        []byte(`{ "total": 9999 }`),
        0644,
    )
    if err != nil {
        t.Fatalf("failed to create test ..usage; %v", err)
    }

    out, err := ReadUsage(f)
    if err != nil {
        t.Fatalf("failed to read test ..usage; %v", err)
    }

    if out.Total != 9999 {
        t.Fatalf("unexpected 'total' value")
    }
}

func TestComputeUsage(t *testing.T) {
    // Mocking up a directory.
    src, err := os.MkdirTemp("", "test-")
    if err != nil {
        t.Fatalf("failed to create tempdir; %v", err)
    }

    expected_size := 0

    msg := "grass,poison"
    err = os.WriteFile(filepath.Join(src, "type"), []byte(msg), 0644)
    expected_size += len(msg)
    if err != nil {
        t.Fatalf("failed to create mock file; %v", err)
    }

    err = os.Mkdir(filepath.Join(src, "evolution"), 0755)
    if err != nil {
        t.Fatalf("failed to create mock directory; %v", err)
    }
    msg = "ivysaur"
    err = os.WriteFile(filepath.Join(src, "evolution", "last"), []byte(msg), 0644)
    expected_size += len(msg)
    if err != nil {
        t.Fatalf("failed to create mock file; %v", err)
    }

    err = os.Mkdir(filepath.Join(src, "moves"), 0755)
    if err != nil {
        t.Fatalf("failed to create mock directory; %v", err)
    }
    err = os.Mkdir(filepath.Join(src, "moves", "grass"), 0755)
    if err != nil {
        t.Fatalf("failed to create mock directory; %v", err)
    }
    msg = "120"
    expected_size += len(msg)
    err = os.WriteFile(filepath.Join(src, "moves", "grass", "solar_beam"), []byte(msg), 0644)
    if err != nil {
        t.Fatalf("failed to create mock file; %v", err)
    }
    msg = "55"
    expected_size += len(msg)
    err = os.WriteFile(filepath.Join(src, "moves", "grass", "razor_leaf"), []byte(msg), 0644)
    if err != nil {
        t.Fatalf("failed to create mock file; %v", err)
    }

    // Actually running some tests.
    total, err := ComputeUsage(src, true)
    if err != nil {
        t.Fatalf("failed to create compute usage; %v", err)
    }
    if total != int64(expected_size) {
        t.Fatalf("sum of file sizes is different from expected (%d, got %d)", expected_size, total)
    }

    err = os.Symlink(
        filepath.Join(src, "moves", "grass", "razor_leaf"), 
        filepath.Join(src, "moves", "grass", "vine_whip"),
    )
    if err != nil {
        t.Fatalf("failed to create mock file; %v", err)
    }

    total, err = ComputeUsage(src, true)
    if err != nil {
        t.Fatalf("failed to create compute usage; %v", err)
    }
    if total != int64(expected_size) {
        t.Fatalf("sum of file sizes is different from expected (%d, got %d) when ignoring soft links", expected_size, total)
    }

    total, err = ComputeUsage(src, false)
    if err != nil {
        t.Fatalf("failed to create compute usage; %v", err)
    }
    if total != int64(expected_size + len(msg)) {
        t.Fatalf("sum of file sizes is different from expected (%d, got %d) when including soft links", expected_size, total)
    }
}

