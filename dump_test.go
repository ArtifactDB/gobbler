package main

import (
    "testing"
    "encoding/json"
    "os"
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

func TestDumpUploadSuccessLog(t *testing.T) {
    f, err := os.MkdirTemp("", "test-")
    if err != nil {
        t.Fatalf("failed to create tempdir; %v", err)
    }

    path := filepath.Join(f, "success")
    err = DumpUploadSuccessLog(path, "foo", "bar")
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
