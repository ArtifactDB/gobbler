package main

import (
    "os"
    "encoding/json"
    "fmt"
    "io/fs"
    "strings"
    "path/filepath"
)

type UsageMetadata struct {
    Total int64 `json:"total"`
}

const UsageFileName = "..usage"

func ReadUsage(path string) (*UsageMetadata, error) {
    usage_path := filepath.Join(path, UsageFileName)

    usage_raw, err := os.ReadFile(usage_path)
    if err != nil {
        return nil, fmt.Errorf("failed to read '" + usage_path + "'; %w", err)
    }

    var output UsageMetadata
    err = json.Unmarshal(usage_raw, &output)
    if err != nil {
        return nil, fmt.Errorf("failed to parse JSON in '" + usage_path + "'; %w", err)
    }

    return &output, nil
}

func ComputeUsage(dir string, skip_symlinks bool) (int64, error) {
    var total int64
    total = 0

    err := filepath.WalkDir(dir, func(path string, info fs.DirEntry, err error) error {
        if err != nil {
            return fmt.Errorf("failed to walk into %q; %w", path, err)
        }

        // Skipping internal files.
        base := filepath.Base(path)
        if strings.HasPrefix(base, "..") {
            return nil
        }

        restat, err := info.Info()
        if err != nil {
            return fmt.Errorf("failed to stat %q; %w", path, err)
        }

        if restat.Mode() & os.ModeSymlink == os.ModeSymlink {
            more_info, err := os.Stat(path)
            if err != nil {
                return fmt.Errorf("failed to stat target of link %q; %w", path, err)
            }
            if more_info.IsDir() {
                return fmt.Errorf("symlinks to directories are not supported (%q); %w", path, err)
            }
            if skip_symlinks {
                return nil
            } 
            total += more_info.Size()
        } else {
            if !info.IsDir() {
                total += restat.Size()
            }
        }

        return nil
    })

    return total, err
}
