package main

import (
    "os"
    "encoding/json"
    "fmt"
    "path/filepath"
)

type LatestMetadata struct {
    Latest string `json:"latest"`
}

const LatestFileName = "..latest"

func ReadLatest(path string) (*LatestMetadata, error) {
    latest_path := filepath.Join(path, LatestFileName)

    latest_raw, err := os.ReadFile(latest_path)
    if err != nil {
        return nil, fmt.Errorf("failed to read '" + latest_path + "'; %w", err)
    }

    var output LatestMetadata
    err = json.Unmarshal(latest_raw, &output)
    if err != nil {
        return nil, fmt.Errorf("failed to parse JSON in '" + latest_path + "'; %w", err)
    }

    return &output, nil
}
