package main

import (
    "encoding/json"
    "os"
    "fmt"
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
