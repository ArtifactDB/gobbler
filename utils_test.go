package main

import (
    "testing"
    "strings"
    "os"
    "fmt"
    "path/filepath"
    "encoding/json"
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

func TestDumpResponse(t *testing.T) {
    response_dir, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create a temporary directory; %v", err)
    }

    basename := "FOO"
    payload := map[string]string { "A": "B", "C": "D" }
    err = dumpResponse(response_dir, basename, &payload)
    if err != nil {
        t.Fatalf("failed to dump a response; %v", err)
    }

    as_str, err := os.ReadFile(filepath.Join(response_dir, basename))
    if err != nil {
        t.Fatalf("failed to read the response; %v", err)
    }

    var roundtrip map[string]string
    err = json.Unmarshal(as_str, &roundtrip)
    if err != nil {
        t.Fatalf("failed to parse the response; %v", err)
    }
    if roundtrip["A"] != payload["A"] || roundtrip["C"] != payload["C"] {
        t.Fatalf("unexpected contents from roundtrip of the response")
    }
}
