package main

import (
    "os"
    "encoding/json"
    "fmt"
    "path/filepath"
)

type Latest struct {
    Latest string `json:"latest"`
}

const LatestFileName = "..latest"

func ReadLatest(path string) (*Latest, error) {
    latest_path := filepath.Join(path, LatestFileName)

    latest_raw, err := os.ReadFile(latest_path)
    if err != nil {
        return nil, fmt.Errorf("failed to read '" + latest_path + "'; %w", err)
    }

    var output Latest
    err = json.Unmarshal(latest_raw, &output)
    if err != nil {
        return nil, fmt.Errorf("failed to parse JSON in '" + latest_path + "'; %w", err)
    }

    return &output, nil
}
