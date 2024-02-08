package main

import (
    "testing"
    "encoding/json"
    "os"
    "time"
    "errors"
    "path/filepath"
)

func has_property(data map[string]string, key, value string) error {
    val, ok := data[key]
    if !ok {
        return errors.New("expected a '" + key + "' string property")
    }
    if val != value {
        return errors.New("expected the '" + key + "' property to equal '" + value + "'")
    }
    return nil
}

func TestDumpFailureLog(t *testing.T) {
    f, err := os.MkdirTemp("", "test-")
    if err != nil {
        t.Fatalf("failed to create tempdir; %v", err)
    }

    path := filepath.Join(f, "failure")
    err = DumpFailureLog(path, errors.New("foo bar"))
    if err != nil {
        t.Fatalf("failed to save the failure log; %v", err)
    }

    contents, err := os.ReadFile(path)
    if err != nil {
        t.Fatalf("failed to read the failure log; %v", err)
    }

    var data map[string]string
    err = json.Unmarshal(contents, &data)
    if err != nil {
        t.Fatalf("failed to parse the failure log; %v", err)
    }

    err = has_property(data, "status", "FAILED")
    if err != nil {
        t.Fatalf(err.Error())
    }

    err = has_property(data, "reason", "foo bar")
    if err != nil {
        t.Fatalf(err.Error())
    }
}

func TestDumpSuccessLog(t *testing.T) {
    f, err := os.MkdirTemp("", "test-")
    if err != nil {
        t.Fatalf("failed to create tempdir; %v", err)
    }

    path := filepath.Join(f, "success")
    err = DumpSuccessLog(path, "foo", "bar")
    if err != nil {
        t.Fatalf("failed to save the success log; %v", err)
    }

    contents, err := os.ReadFile(path)
    if err != nil {
        t.Fatalf("failed to read the success log; %v", err)
    }

    var data map[string]string
    err = json.Unmarshal(contents, &data)
    if err != nil {
        t.Fatalf("failed to parse the success log; %v", err)
    }

    err = has_property(data, "status", "SUCCESS")
    if err != nil {
        t.Fatalf(err.Error())
    }

    err = has_property(data, "project", "foo")
    if err != nil {
        t.Fatalf(err.Error())
    }

    err = has_property(data, "version", "bar")
    if err != nil {
        t.Fatalf(err.Error())
    }
}

func TestDumpVersionMetadata(t *testing.T) {
    f, err := os.MkdirTemp("", "test-")
    if err != nil {
        t.Fatalf("failed to create tempdir; %v", err)
    }

    path := filepath.Join(f, "metadata")
    err = DumpVersionMetadata(path, "aaron")
    if err != nil {
        t.Fatalf("failed to save the version metadata; %v", err)
    }

    contents, err := os.ReadFile(path)
    if err != nil {
        t.Fatalf("failed to read the version metadata; %v", err)
    }

    var data map[string]string
    err = json.Unmarshal(contents, &data)
    if err != nil {
        t.Fatalf("failed to parse the version metadata; %v", err)
    }

    err = has_property(data, "uploader_id", "aaron")
    if err != nil {
        t.Fatalf(err.Error())
    }

    utime, ok := data["uploaded_at"]
    if !ok {
        t.Fatalf("expected an 'uploaded_at' string property")
    }
    parsed, err := time.Parse(time.RFC3339, utime)
    if err != nil {
        t.Fatalf("failed to parse the upload time; %v", err)
    }
    time.Sleep(10 * time.Millisecond)
    if (!time.Now().After(parsed)) {
        t.Fatalf("upload time should be after the current time")
    }
}
