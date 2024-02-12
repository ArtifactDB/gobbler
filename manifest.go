package main

import (
    "os"
    "encoding/json"
    "path/filepath"
    "fmt"
)

type linkMetadata struct {
    Project string `json:"project"`
    Asset string `json:"asset"`
    Version string `json:"version"`
    Path string `json:"path"`
    Ancestor *linkMetadata `json:"ancestor,omitempty"`
}

type manifestEntry struct {
    Size int64 `json:"size"`
    Md5sum string `json:"md5sum"`
    Link *linkMetadata `json:"link,omitempty"`
}

const manifestFileName = "..manifest"
const linksFileName = "..links"

func readManifest(path string) (map[string]manifestEntry, error) {
    manifest_path := filepath.Join(path, manifestFileName)

    manifest_raw, err := os.ReadFile(manifest_path)
    if err != nil {
        return nil, fmt.Errorf("cannot read '" + manifest_path + "'; %w", err)
    }

    var info map[string]manifestEntry
    err = json.Unmarshal(manifest_raw, &info)
    if err != nil {
        return nil, fmt.Errorf("cannot parse JSON in '" + manifest_path + "'; %w", err)
    }

    return info, nil
}
