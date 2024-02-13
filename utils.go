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

func dumpJson(path string, output interface{}) error {
    str, err := json.MarshalIndent(output, "", "    ")
    if err != nil {
        return fmt.Errorf("failed to marshal JSON; %w", err)
    }

    err = os.WriteFile(path, str, 0644)
    if err != nil {
        return fmt.Errorf("failed to write to %q; %w", path, err)
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

func dumpLog(registry string, output interface{}) error {
    path := time.Now().Format(time.RFC3339) + "_" + strconv.Itoa(100000 + rand.Intn(900000))
    return dumpJson(filepath.Join(registry, logDirName, path), output)
}

func dumpResponse(response_dir, reqname string, content interface{}) error {
    // Using the save-and-rename paradigm to avoid clients picking up partial writes.
    temp, err := os.CreateTemp(response_dir, "TEMP")
    if err != nil {
        return fmt.Errorf("failed to create temporary file for response to %q; %w", reqname, err)
    }

    is_closed := false
    defer func() {
        if !is_closed {
            temp.Close()
        }
    }()

    as_str, err := json.MarshalIndent(content, "", "    ")
    if err != nil {
        return fmt.Errorf("failed to marshal JSON for response to %q; %w", reqname, err)
    }

    _, err = temp.Write(as_str)
    if err != nil {
        return fmt.Errorf("failed to write JSON for response to %q; %w", reqname, err)
    }

    temp_name := temp.Name()
    is_closed = true
    err = temp.Close()
    if err != nil {
        return fmt.Errorf("failed to close file for response to %q; %w", reqname, err)
    }

    logpath := filepath.Join(response_dir, reqname)
    err = os.Rename(temp_name, logpath)
    if err != nil {
        return fmt.Errorf("failed to rename response to %q; %w", reqname, err)
    }

    return nil
}
