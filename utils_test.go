package main

import (
    "testing"
    "strings"
    "os"
    "fmt"
)

func dumpRequest(request_type, request_string string) (string, error) {
    handle, err := os.CreateTemp("", "request-" + request_type + "-")
    if err != nil {
        return "", fmt.Errorf("failed to create temp file; %w", err)
    }

    _, err = handle.WriteString(request_string)
    if err != nil {
        return "", fmt.Errorf("failed to write string; %w", err)
    }

    reqname := handle.Name()
    err = handle.Close()
    if err != nil {
        return "", fmt.Errorf("failed to close file; %w", err)
    }

    return reqname, nil
}

func verifyFileContents(path, contents string) error {
    observed, err := os.ReadFile(path)
    if err != nil {
        return fmt.Errorf("failed to read %q; %w", path, err)
    }
    if string(observed) != contents {
        return fmt.Errorf("unexpected contents of %q; %w", path, err)
    }
    return nil
}

func TestIsBadName(t *testing.T) {
    var err error

    err = isBadName("..foo")
    if err == nil || !strings.Contains(err.Error(), "..")  {
        t.Fatal("failed to stop on '..'")
    }

    err = isBadName("")
    if err == nil || !strings.Contains(err.Error(), "empty") {
        t.Fatal("failed to stop on an empty name")
    }

    err = isBadName("asda/a")
    if err == nil || !strings.Contains(err.Error(), "/") {
        t.Fatal("failed to stop in the presence of a forward slash")
    }

    err = isBadName("asda\\asdasd")
    if err == nil || !strings.Contains(err.Error(), "\\") {
        t.Fatal("failed to stop in the presence of a backslash")
    }
}
