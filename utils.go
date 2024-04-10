package main

import (
    "encoding/json"
    "os"
    "fmt"
    "errors"
    "strings"
    "time"
    "math/rand"
    "strconv"
    "path/filepath"
)

type globalConfiguration struct {
    Registry string
    Administrators []string
    Locks pathLocks
}

func newGlobalConfiguration(registry string) globalConfiguration {
    return globalConfiguration{ 
        Registry: registry, 
        Administrators: []string{},
        Locks: newPathLocks(),
    }
}

type readRequestError struct {
    Cause error
}

func (r *readRequestError) Error() string {
    return r.Cause.Error()
}

func dumpJson(path string, content interface{}) error {
    // Using the save-and-rename paradigm to avoid clients picking up partial writes.
    temp, err := os.CreateTemp(filepath.Dir(path), ".temp*.json")
    if err != nil {
        return fmt.Errorf("failed to create temporary file when saving %q; %w", path, err)
    }

    is_closed := false
    defer func() {
        if !is_closed {
            temp.Close()
        }
    }()

    err = os.Chmod(temp.Name(), 0644)
    if err != nil {
        return fmt.Errorf("failed to set temporary file permissions when saving %q; %w", path, err);
    }

    as_str, err := json.MarshalIndent(content, "", "    ")
    if err != nil {
        return fmt.Errorf("failed to marshal JSON to save to %q; %w", path, err)
    }

    _, err = temp.Write(as_str)
    if err != nil {
        return fmt.Errorf("failed to write JSON to temporary file for %q; %w", path, err)
    }

    temp_name := temp.Name()
    is_closed = true
    err = temp.Close()
    if err != nil {
        return fmt.Errorf("failed to close temporary file when saving to %q; %w", path, err)
    }

    err = os.Rename(temp_name, path)
    if err != nil {
        return fmt.Errorf("failed to rename temporary file to %q; %w", path, err)
    }

    return nil
}

func isBadName(name string) error {
    if len(name) == 0 {
        return errors.New("name cannot be empty")
    }
    if strings.Contains(name, "/") || strings.Contains(name, "\\") {
        return errors.New("name cannot contain '/' or '\\'")
    }
    if strings.HasPrefix(name, "..") {
        return errors.New("name cannot start with '..'")
    }
    return nil
}

func isMissingOrBadName(name *string) error {
    if name == nil {
        return errors.New("missing name")
    } else {
        return isBadName(*name)
    }
}

const logDirName = "..logs"

func dumpLog(registry string, content interface{}) error {
    path := time.Now().Format(time.RFC3339) + "_" + strconv.Itoa(100000 + rand.Intn(900000))
    return dumpJson(filepath.Join(registry, logDirName, path), content)
}
