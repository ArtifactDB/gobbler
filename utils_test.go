package main

import (
    "testing"
    "strings"
    "os"
    "fmt"
    "path/filepath"
    "encoding/json"
    "errors"
    "net/http"
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

func constructMockRegistry() (string, error) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        return "", fmt.Errorf("failed to create the registry; %w", err)
    }

    err = os.Mkdir(filepath.Join(reg, logDirName), 0755)
    if err != nil {
        return "", fmt.Errorf("failed to create log subdirectory; %w", err)
    }

    return reg, nil
}

type logEntry struct {
    Type string `json:"type"`
    Project *string `json:"project"`
    Asset *string `json:"asset"`
    Version *string `json:"version"`
    Latest *bool `json:"latest"`
}

func readAllLogs(registry string) ([]logEntry, error) {
    logdir := filepath.Join(registry, logDirName)
    logs, err := os.ReadDir(logdir)
    if err != nil {
        return nil, fmt.Errorf("failed to list the log directory contents; %w", err)
    }

    output := make([]logEntry, len(logs))
    for i, l := range logs {
        logpath := filepath.Join(logdir, l.Name())
        content_raw, err := os.ReadFile(logpath)
        if err != nil {
            return nil, fmt.Errorf("failed to read the log at %q; %w", logpath, err)
        }

        err = json.Unmarshal(content_raw, &(output[i]))
        if err != nil {
            return nil, fmt.Errorf("failed to parse the log at %q; %w", logpath, err)
        }
    }

    return output, nil
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

func TestCheckProjectExists(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatal(err)
    }

    err = checkProjectExists(filepath.Join(reg, "doesnt_exist"), "foo")
    var http_err *httpError
    if !errors.As(err, &http_err) {
        t.Error("expected a HTTP error")
    } else if http_err.Status != http.StatusNotFound {
        t.Error("expected a HTTP error with a 404")
    }

    err = checkProjectExists(reg, "foo")
    if err != nil {
        t.Error("no error expected here")
    }
}

func TestCheckAssetExists(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatal(err)
    }

    err = checkAssetExists(filepath.Join(reg, "doesnt_exist"), "bar", "foo")
    var http_err *httpError
    if !errors.As(err, &http_err) {
        t.Error("expected a HTTP error")
    } else if http_err.Status != http.StatusNotFound {
        t.Error("expected a HTTP error with a 404")
    }

    err = checkAssetExists(reg, "bar", "foo")
    if err != nil {
        t.Error("no error expected here")
    }
}

func TestCheckVersionExists(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatal(err)
    }

    err = checkVersionExists(filepath.Join(reg, "doesnt_exist"), "whee", "bar", "foo")
    var http_err *httpError
    if !errors.As(err, &http_err) {
        t.Error("expected a HTTP error")
    } else if http_err.Status != http.StatusNotFound {
        t.Error("expected a HTTP error with a 404")
    }

    err = checkVersionExists(reg, "whee", "bar", "foo")
    if err != nil {
        t.Error("no error expected here")
    }
}
