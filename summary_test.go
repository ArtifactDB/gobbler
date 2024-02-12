package main

import (
    "testing"
    "os"
    "path/filepath"
)

func TestReadSummary(t *testing.T) {
    f, err := os.MkdirTemp("", "test-")
    if err != nil {
        t.Fatalf("failed to create tempdir; %v", err)
    }

    err = os.WriteFile(
        filepath.Join(f, summaryFileName),
        []byte(`
{ 
    "upload_user_id": "aaron",
    "upload_start": "2020-02-02T02:20:02Z",
    "upload_finish": "2021-12-20T21:20:11Z"
}`),
        0644,
    )
    if err != nil {
        t.Fatalf("failed to create test summary; %v", err)
    }

    out, err := readSummary(f)
    if err != nil {
        t.Fatalf("failed to read test summary; %v", err)
    }

    if out.UploadUserId != "aaron" || out.UploadStart != "2020-02-02T02:20:02Z" || out.UploadFinish != "2021-12-20T21:20:11Z" || out.OnProbation != nil {
        t.Fatalf("unexpected values in the test summary; %v", err)
    }

    // Trying again with the probational flag.
    err = os.WriteFile(
        filepath.Join(f, summaryFileName),
        []byte(`
{ 
    "upload_user_id": "aaron",
    "upload_start": "2020-02-02T02:20:02Z",
    "upload_finish": "2021-12-20T21:20:11Z",
    "on_probation": true
}`),
        0644,
    )
    if err != nil {
        t.Fatalf("failed to create test summary; %v", err)
    }

    out, err = readSummary(f)
    if err != nil {
        t.Fatalf("failed to read test summary; %v", err)
    }

    if out.UploadUserId != "aaron" || out.UploadStart != "2020-02-02T02:20:02Z" || out.UploadFinish != "2021-12-20T21:20:11Z" || !out.IsProbational() {
        t.Fatalf("unexpected values in the test summary; %v", err)
    }
}
