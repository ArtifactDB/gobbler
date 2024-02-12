package main

import (
    "fmt"
    "time"
    "path/filepath"
)

func uploadHandler(reqpath, registry string, administrators []string) (*Configuration, error) {
    request, err := ReadUploadRequest(reqpath)
    if err != nil {
        return nil, fmt.Errorf("failed to parse request at %q; %w", reqpath, err)
    }

    upload_start := time.Now()
    source := *(request.Source)

    config, err := Configure(request, registry, administrators)
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
    lock_path := filepath.Join(project_dir, LockFileName)
    handle, err := Lock(lock_path, 1000 * time.Second)
    if err != nil {
        return nil, fmt.Errorf("failed to lock project directory %q; %w", project_dir, err)
    }
    defer Unlock(handle)

    asset_dir := filepath.Join(project_dir, config.Asset)
    if !config.OnProbation {
        latest := LatestMetadata { Latest: config.Version }
        latest_path := filepath.Join(asset_dir, LatestFileName)
        err := dumpJson(latest_path, &latest)
        if err != nil {
            return nil, fmt.Errorf("failed to save latest version for %q; %w", asset_dir, err)
        }
    }

    version_dir := filepath.Join(asset_dir, config.Version)
    {
        summary := SummaryMetadata {
            UploadUserId: config.User,
            UploadStart: upload_start.Format(time.RFC3339),
            UploadFinish: time.Now().Format(time.RFC3339),
        }
        if config.OnProbation {
            summary.OnProbation = &(config.OnProbation)
        }

        summary_path := filepath.Join(version_dir, SummaryFileName)
        err := dumpJson(summary_path, &summary)
        if err != nil {
            return nil, fmt.Errorf("failed to save summary for %q; %w", asset_dir, err)
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

        usage_path := filepath.Join(project_dir, UsageFileName)
        err = dumpJson(usage_path, &usage)
        if err != nil {
            return nil, fmt.Errorf("failed to save usage for %q; %w", project_dir, err)
        }
    }

    return config, nil
}
