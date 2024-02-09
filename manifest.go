package main

import (
    "os"
    "encoding/json"
    "path/filepath"
    "fmt"
)

type LinkMetadata struct {
    Project string `json:"project"`
    Asset string `json:"asset"`
    Version string `json:"version"`
    Path string `json:"path"`
    Ancestor *LinkMetadata `json:"ancestor,omitempty"`
}

type ManifestEntry struct {
    Size int64 `json:"size"`
    Md5sum string `json:"md5sum"`
    Link *LinkMetadata `json:"link"`
}

const ManifestFileName = "..manifest"
const LinksFileName = "..links"

func ReadManifest(path string) (map[string]ManifestEntry, error) {
    manifest_path := filepath.Join(path, ManifestFileName)

    manifest_raw, err := os.ReadFile(manifest_path)
    if err != nil {
        return nil, fmt.Errorf("cannot read '" + manifest_path + "'; %w", err)
    }

    var info map[string]ManifestEntry
    err = json.Unmarshal(manifest_raw, &info)
    if err != nil {
        return nil, fmt.Errorf("cannot parse JSON in '" + manifest_path + "'; %w", err)
    }

    return info, nil
}
