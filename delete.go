package main

import (
    "os"
    "fmt"
    "encoding/json"
    "path/filepath"
    "time"
)

func deleteProjectHandler(reqpath string, globals *globalConfiguration) error {
    req_user, err := identifyUser(reqpath)
    if err != nil {
        return fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }
    if !isAuthorizedToAdmin(req_user, globals.Administrators) {
        return fmt.Errorf("user %q is not authorized to delete a project", req_user)
    }

    incoming := struct {
        Project *string `json:"project"`
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

        err = isMissingOrBadName(incoming.Project)
        if err != nil {
            return fmt.Errorf("invalid 'project' property in %q; %w", reqpath, err)
        }
    }

    project_dir := filepath.Join(globals.Registry, *(incoming.Project))
    err = os.RemoveAll(project_dir)
    if err != nil {
        return fmt.Errorf("failed to delete %s; %v", project_dir, err)
    }

    payload := map[string]string { 
        "type": "delete-project", 
        "project": *(incoming.Project),
    }
    err = dumpLog(globals.Registry, &payload)
    if err != nil {
        return fmt.Errorf("failed to create log for project deletion; %w", err)
    }

    return nil
}

func deleteAssetHandler(reqpath string, globals *globalConfiguration) error {
    req_user, err := identifyUser(reqpath)
    if err != nil {
        return fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }
    if !isAuthorizedToAdmin(req_user, globals.Administrators) {
        return fmt.Errorf("user %q is not authorized to delete a project", req_user)
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

        err = isMissingOrBadName(incoming.Project)
        if err != nil {
            return fmt.Errorf("invalid 'project' property in %q; %w", reqpath, err)
        }
        err = isMissingOrBadName(incoming.Asset)
        if err != nil {
            return fmt.Errorf("invalid 'asset' property in %q; %w", reqpath, err)
        }
    }

    project_dir := filepath.Join(globals.Registry, *(incoming.Project))
    err = globals.Locks.LockPath(project_dir, 1000 * time.Second)
    if err != nil {
        return fmt.Errorf("failed to lock project directory %q; %w", project_dir, err)
    }
    defer globals.Locks.UnlockPath(project_dir)

    asset_dir := filepath.Join(project_dir, *(incoming.Asset))
    to_free, err := computeUsage(asset_dir, true)
    if err != nil {
        return fmt.Errorf("failed to compute usage for %s; %v", asset_dir, err)
    }
    usage, err := readUsage(project_dir)
    if err != nil {
        return fmt.Errorf("failed to read usage for %s; %v", project_dir, err)
    }

    err = os.RemoveAll(asset_dir)
    if err != nil {
        return fmt.Errorf("failed to delete %s; %v", asset_dir, err)
    }

    usage.Total -= to_free
    if usage.Total < 0 {
        usage.Total = 0
    }
    err = dumpJson(filepath.Join(project_dir, usageFileName), &usage)
    if err != nil {
        return fmt.Errorf("failed to update usage for %s; %v", project_dir, err)
    }

    payload := map[string]string { 
        "type": "delete-asset", 
        "project": *(incoming.Project),
        "asset": *(incoming.Asset),
    }
    err = dumpLog(globals.Registry, &payload)
    if err != nil {
        return fmt.Errorf("failed to create log for asset deletion; %w", err)
    }

    return nil
}

func deleteVersionHandler(reqpath string, globals *globalConfiguration) error {
    req_user, err := identifyUser(reqpath)
    if err != nil {
        return fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }
    if !isAuthorizedToAdmin(req_user, globals.Administrators) {
        return fmt.Errorf("user %q is not authorized to delete a project", req_user)
    }

    incoming := struct {
        Project *string `json:"project"`
        Asset *string `json:"asset"`
        Version *string `json:"version"`
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

        err = isMissingOrBadName(incoming.Project)
        if err != nil {
            return fmt.Errorf("invalid 'project' property in %q; %w", reqpath, err)
        }
        err = isMissingOrBadName(incoming.Asset)
        if err != nil {
            return fmt.Errorf("invalid 'asset' property in %q; %w", reqpath, err)
        }
        err = isMissingOrBadName(incoming.Version)
        if err != nil {
            return fmt.Errorf("invalid 'version' property in %q; %w", reqpath, err)
        }
    }

    // We lock the project directory as (i) it's convention to lock the entire
    // project even if we're mutating a single asset and (ii) we need to update
    // the usage anyway so we'd have to obtain this lock eventually.
    project_dir := filepath.Join(globals.Registry, *(incoming.Project))
    err = globals.Locks.LockPath(project_dir, 1000 * time.Second)
    if err != nil {
        return fmt.Errorf("failed to lock project directory %q; %w", project_dir, err)
    }
    defer globals.Locks.UnlockPath(project_dir)

    asset_dir := filepath.Join(project_dir, *(incoming.Asset))
    version_dir := filepath.Join(asset_dir, *(incoming.Version))
    to_free, err := computeUsage(version_dir, true)
    if err != nil {
        return fmt.Errorf("failed to compute usage for %s; %v", version_dir, err)
    }
    usage, err := readUsage(project_dir)
    if err != nil {
        return fmt.Errorf("failed to read usage for %s; %v", project_dir, err)
    }

    err = os.RemoveAll(version_dir)
    if err != nil {
        return fmt.Errorf("failed to delete %s; %v", asset_dir, err)
    }

    usage.Total -= to_free
    if usage.Total < 0 {
        usage.Total = 0
    }
    err = dumpJson(filepath.Join(project_dir, usageFileName), &usage)
    if err != nil {
        return fmt.Errorf("failed to update usage for %s; %v", project_dir, err)
    }

    _, err = refreshLatest(asset_dir)
    if err != nil {
        return fmt.Errorf("failed to update the latest version for %s; %v", asset_dir, err)
    }

    payload := map[string]string { 
        "type": "delete-version", 
        "project": *(incoming.Project),
        "asset": *(incoming.Asset),
        "version": *(incoming.Version),
    }
    err = dumpLog(globals.Registry, &payload)
    if err != nil {
        return fmt.Errorf("failed to create log for version deletion; %w", err)
    }

    return nil
}

