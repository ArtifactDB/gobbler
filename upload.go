package main

import (
    "fmt"
    "time"
    "encoding/json"
    "os"
    "path/filepath"
)

func Upload(source, registry string) (*Configuration, error) {
    upload_start := time.Now()

    config, err := Configure(source, registry)
    if err != nil {
        return nil, fmt.Errorf("failed to configure upload for %q; %w", source, err)
    }

    err = Transfer(source, registry, config.Project, config.Asset, config.Version)
    if err != nil {
        return nil, fmt.Errorf("failed to transfer files from %q; %w", source, err)
    }

    // Locking the entire project directory to write out shared metadata.
    // Technically we don't need to do this until we get around to writing
    // ..usage, but it's just a bit safer that way, and besides: who is going
    // to upload to the same proejct at the same time?
    project_dir := filepath.Join(registry, config.Project)
    lock_path := filepath.Join(project_dir, "..LOCK")
    handle, err := Lock(lock_path, 1000 * time.Second)
    if err != nil {
        return nil, fmt.Errorf("failed to lock project directory %q; %w", project_dir, err)
    }
    defer Unlock(handle)

    asset_dir := filepath.Join(project_dir, config.Asset)
    if true {
        latest := LatestMetadata {
            Latest: config.Version,
        }

        latest_str, err := json.MarshalIndent(&latest, "", "    ")
        if err != nil {
            return nil, fmt.Errorf("failed to stringify latest for upload from %q; %w", source, err)
        }

        latest_path := filepath.Join(asset_dir, LatestFileName)
        err = os.WriteFile(latest_path, latest_str, 0644)
        if err != nil {
            return nil, fmt.Errorf("failed to write to %q; %w", latest_path, err)
        }
    }

    version_dir := filepath.Join(asset_dir, config.Version)
    {
        summary := SummaryMetadata {
            UploadUserId: config.User,
            UploadStart: upload_start.Format(time.RFC3339),
            UploadFinish: time.Now().Format(time.RFC3339),
        }

        summary_path := filepath.Join(version_dir, SummaryFileName)
        summary_str, err := json.MarshalIndent(&summary, "", "    ")
        if err != nil {
            return nil, fmt.Errorf("failed to stringify summary for %q; %w", summary_path, err)
        }

        err = os.WriteFile(summary_path, summary_str, 0644)
        if err != nil {
            return nil, fmt.Errorf("failed to write to %q; %w", summary_path, err)
        }
    }

    {
        extra, err := ComputeUsage(version_dir, true)
        if err != nil {
            return nil, fmt.Errorf("failed to compute usage for the new version at %q; %w", version_dir, err)
        }

        usage, err := ReadUsage(project_dir)
        if err != nil {
            return nil, fmt.Errorf("failed to read existing usage for project %q; %w", config.Project, err)
        }

        usage.Total += extra
        usage_path := filepath.Join(version_dir, UsageFileName)
        usage_str, err := json.MarshalIndent(&usage, "", "    ")
        if err != nil {
            return nil, fmt.Errorf("failed to stringify usage for %q; %w", usage_path, err)
        }

        err = os.WriteFile(usage_path, usage_str, 0644)
        if err != nil {
            return nil, fmt.Errorf("failed to write to %q; %w", usage_path, err)
        }
    }

    return config, nil
}
