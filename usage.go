package main

import (
    "os"
    "encoding/json"
    "fmt"
    "path/filepath"
    "net/http"
)

type usageMetadata struct {
    Total int64 `json:"total"`
}

const usageFileName = "..usage"

func readUsage(path string) (*usageMetadata, error) {
    usage_path := filepath.Join(path, usageFileName)

    usage_raw, err := os.ReadFile(usage_path)
    if err != nil {
        return nil, fmt.Errorf("failed to read '" + usage_path + "'; %w", err)
    }

    var output usageMetadata
    err = json.Unmarshal(usage_raw, &output)
    if err != nil {
        return nil, fmt.Errorf("failed to parse JSON in '" + usage_path + "'; %w", err)
    }

    return &output, nil
}

func computeProjectUsage(path string) (int64, error) {
    var total int64
    total = 0

    assets, err := os.ReadDir(path)
    if err != nil {
        return total, fmt.Errorf("failed to list assets; %w", err)
    }

    for _, aentry := range assets {
        if !aentry.IsDir() {
            continue
        }

        asset := aentry.Name()
        asize, err := computeAssetUsage(filepath.Join(path, asset))
        if err != nil {
            return total, fmt.Errorf("failed to get usage for asset %q; %w", asset, err) 
        }

        total += asize
    }

    return total, nil
}

func computeAssetUsage(path string) (int64, error) {
    var total int64
    total = 0

    versions, err := os.ReadDir(path)
    if err != nil {
        return total, fmt.Errorf("failed to list versions; %w", err)
    }

    for _, ventry := range versions {
        if !ventry.IsDir() {
            continue
        }

        version := ventry.Name()
        vsize, err := computeVersionUsage(filepath.Join(path, version))
        if err != nil {
            return total, fmt.Errorf("failed to get usage for version %q; %w", version, err) 
        }

        total += vsize
    }

    return total, nil
}

func computeVersionUsage(path string) (int64, error) {
    var total int64
    total = 0

    man, err := readManifest(path)
    if err != nil {
        return total, fmt.Errorf("failed to open manifest; %w", err)
    }

    for _, mm := range man {
        if mm.Link == nil {
            total += mm.Size
        }
    }

    return total, err
}

func refreshUsageHandler(reqpath string, globals *globalConfiguration) (*usageMetadata, error) {
    source_user, err := identifyUser(reqpath)
    if err != nil {
        return nil, fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }

    if !isAuthorizedToAdmin(source_user, globals.Administrators) {
        return nil, newHttpError(http.StatusForbidden, fmt.Errorf("user %q is not authorized to refresh the latest version (%q)", source_user, reqpath))
    }

    incoming := struct {
        Project *string `json:"project"`
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
    }

    rlock, err := lockDirectoryShared(globals, globals.Registry)
    if err != nil {
        return nil, fmt.Errorf("failed to lock the registry %q; %w", globals.Registry, err)
    }
    defer rlock.Unlock()

    project := *(incoming.Project)
    project_dir := filepath.Join(globals.Registry, project)
    if err := checkProjectExists(project_dir, project); err != nil {
        return nil, err
    }

    plock, err := lockDirectoryExclusive(globals, project_dir)
    if err != nil {
        return nil, fmt.Errorf("failed to lock the project directory %q; %w", project_dir, err)
    }
    defer plock.Unlock()

    new_usage, err := computeProjectUsage(project_dir)
    if err != nil {
        return nil, fmt.Errorf("failed to compute usage for %q; %w", *(incoming.Project), err)
    }

    usage_path := filepath.Join(project_dir, usageFileName)
    usage_meta := usageMetadata{ Total: new_usage }
    err = dumpJson(usage_path, &usage_meta)
    if err != nil {
        return nil, fmt.Errorf("failed to write new usage for %q; %w", *(incoming.Project), err)
    }

    return &usage_meta, nil
}
