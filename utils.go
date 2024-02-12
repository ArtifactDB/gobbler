package main

import (
    "encoding/json"
    "os"
    "fmt"
    "errors"
    "strings"
)

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
