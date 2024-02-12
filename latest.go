package main

import (
    "os"
    "encoding/json"
    "fmt"
    "path/filepath"
    "time"
    "strings"
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

func refreshLatest(asset_dir string) error {
    entries, err := os.ReadDir(asset_dir)
    if err != nil {
        return fmt.Errorf("failed to list versions in %q; %w", asset_dir, err)
    }

    found := false
    var most_recent time.Time
    var most_recent_name string

    for _, entry := range entries {
        if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
            full_path := filepath.Join(asset_dir, entry.Name())
            summ, err := ReadSummary(full_path)
            if err != nil {
                return fmt.Errorf("failed to read summary from %q; %w", full_path, err)
            }
            if summ.IsProbational() {
                continue
            }

            as_time, err := time.Parse(time.RFC3339, summ.UploadFinish)
            if err != nil {
                return fmt.Errorf("could not parse 'upload_finish' from %q; %w", full_path, err)
            }

            if !found || most_recent.Before(as_time) {
                most_recent = as_time
                most_recent_name = entry.Name()
                found = true
            }
        }
    }

    latest_path := filepath.Join(asset_dir, LatestFileName)
    if found {
        output := LatestMetadata { Latest: most_recent_name }
        err := dump_json(latest_path, &output)
        if err != nil {
            return fmt.Errorf("failed to save '..latest' to %q; %w", asset_dir, err)
        }
    } else {
        err := os.Remove(latest_path)
        if err != nil {
            return fmt.Errorf("failed to remove %q; %w", latest_path, err)
        }
    }

    return nil
}

func refreshLatestHandler(reqpath, registry string, administrators []string) error {
    source_user, err := IdentifyUser(reqpath)
    if err != nil {
        return fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }

    if !IsAuthorizedToAdmin(source_user, administrators) {
        return fmt.Errorf("user %q is not authorized to refreseh the latest version (%q)", source_user, reqpath)
    }

    incoming := struct {
        Project *string `json:"project"`
        Asset *string `json:"asset"`
    }{}
    {
        handle, err := os.ReadFile(reqpath)
        if err != nil {
            return fmt.Errorf("failed to read %q; %w", reqpath, err)
        }

        err = json.Unmarshal(handle, &incoming)
        if err != nil {
            return fmt.Errorf("failed to parse JSON from %q; %w", reqpath, err)
        }
    }

    if incoming.Project == nil {
        return fmt.Errorf("expected a 'project' property in %q", reqpath)
    }
    if incoming.Asset == nil {
        return fmt.Errorf("expected an 'asset' property in %q", reqpath)
    }

    asset_dir := filepath.Join(registry, *(incoming.Project), *(incoming.Asset))
    lock_path := filepath.Join(asset_dir, LockFileName)
    handle, err := Lock(lock_path, 1000 * time.Second)
    if err != nil {
        return fmt.Errorf("failed to acquire the lock on the asset directory %q; %w", asset_dir, err)
    }
    defer Unlock(handle)

    return refreshLatest(asset_dir)
}
