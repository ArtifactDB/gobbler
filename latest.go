package main

import (
    "os"
    "encoding/json"
    "fmt"
    "path/filepath"
    "time"
    "net/http"
    "context"
)

type latestMetadata struct {
    Version string `json:"version"`
}

const latestFileName = "..latest"

func readLatest(path string) (*latestMetadata, error) {
    latest_path := filepath.Join(path, latestFileName)

    latest_raw, err := os.ReadFile(latest_path)
    if err != nil {
        return nil, fmt.Errorf("failed to read '" + latest_path + "'; %w", err)
    }

    var output latestMetadata
    err = json.Unmarshal(latest_raw, &output)
    if err != nil {
        return nil, fmt.Errorf("failed to parse JSON in '" + latest_path + "'; %w", err)
    }

    return &output, nil
}

func refreshLatest(asset_dir string) (*latestMetadata, error) {
    entries, err := listUserDirectories(asset_dir)
    if err != nil {
        return nil, fmt.Errorf("failed to list versions in %q; %w", asset_dir, err)
    }

    found := false
    var most_recent time.Time
    var most_recent_name string

    for _, entry := range entries {
        full_path := filepath.Join(asset_dir, entry)
        summ, err := readSummary(full_path)
        if err != nil {
            return nil, fmt.Errorf("failed to read summary from %q; %w", full_path, err)
        }
        if summ.IsProbational() {
            continue
        }

        as_time, err := time.Parse(time.RFC3339, summ.UploadFinish)
        if err != nil {
            return nil, fmt.Errorf("could not parse 'upload_finish' from %q; %w", full_path, err)
        }

        if !found || most_recent.Before(as_time) {
            most_recent = as_time
            most_recent_name = entry
            found = true
        }
    }

    latest_path := filepath.Join(asset_dir, latestFileName)
    if found {
        output := latestMetadata { Version: most_recent_name }
        err := dumpJson(latest_path, &output)
        if err != nil {
            return nil, fmt.Errorf("failed to update latest version in %q; %w", asset_dir, err)
        }
        return &output, nil
    } else {
        err := os.RemoveAll(latest_path)
        if err != nil {
            return nil, fmt.Errorf("failed to remove %q; %w", latest_path, err)
        }
        return nil, nil
    }
}

func refreshLatestHandler(reqpath string, globals *globalConfiguration, ctx context.Context) (*latestMetadata, error) {
    source_user, err := identifyUser(reqpath)
    if err != nil {
        return nil, fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }

    if !isAuthorizedToAdmin(source_user, globals.Administrators) {
        return nil, newHttpError(http.StatusForbidden, fmt.Errorf("user %q is not authorized to refresh the latest version (%q)", source_user, reqpath))
    }

    incoming := struct {
        Project *string `json:"project"`
        Asset *string `json:"asset"`
    }{}
    {
        handle, err := os.ReadFile(reqpath)
        if err != nil {
            return nil, fmt.Errorf("failed to read %q; %w", reqpath, err)
        }

        err = json.Unmarshal(handle, &incoming)
        if err != nil {
            return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("failed to parse JSON from %q; %w", reqpath, err))
        }

        err = isMissingOrBadName(incoming.Project) 
        if err != nil {
            return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'project' property in %q; %w", reqpath, err))
        }

        err = isMissingOrBadName(incoming.Asset) 
        if err != nil {
            return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'asset' property in %q; %w", reqpath, err))
        }
    }

    rlock, err := lockDirectoryShared(globals.Registry, globals, ctx)
    if err != nil {
        return nil, fmt.Errorf("failed to lock the registry %q; %w", globals.Registry, err)
    }
    defer rlock.Unlock(globals)

    rnnlock, err := lockDirectoryNewDirShared(globals.Registry, globals, ctx)
    if err != nil {
        return nil, fmt.Errorf("failed to lock the registry %q; %w", globals.Registry, err)
    }
    defer rnnlock.Unlock(globals)

    project := *(incoming.Project)
    project_dir := filepath.Join(globals.Registry, project)
    if err := checkProjectExists(project_dir, project); err != nil {
        return nil, err
    }
    rnnlock.Unlock(globals) // no need to hold this lock once we have safely entered the subdirectory.

    plock, err := lockDirectoryShared(project_dir, globals, ctx)
    if err != nil {
        return nil, fmt.Errorf("failed to lock project directory %q; %w", project_dir, err)
    }
    defer plock.Unlock(globals)

    pnnlock, err := lockDirectoryNewDirShared(project_dir, globals, ctx)
    if err != nil {
        return nil, fmt.Errorf("failed to lock project directory %q; %w", project_dir, err)
    }
    defer pnnlock.Unlock(globals)

    asset := *(incoming.Asset)
    asset_dir := filepath.Join(project_dir, asset)
    if err := checkAssetExists(asset_dir, asset, project); err != nil {
        return nil, err
    }
    pnnlock.Unlock(globals) // no need to hold this lock once we have safely entered the subdirectory.

    alock, err := lockDirectoryExclusive(asset_dir, globals, ctx)
    if err != nil {
        return nil, fmt.Errorf("failed to lock asset directory %q; %w", asset_dir, err)
    }
    defer alock.Unlock(globals)

    output, err := refreshLatest(asset_dir)
    return output, err
}
