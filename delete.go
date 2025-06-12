package main

import (
    "os"
    "fmt"
    "encoding/json"
    "path/filepath"
    "errors"
    "net/http"
    "context"
)

func deleteProjectHandler(reqpath string, globals *globalConfiguration, ctx context.Context) error {
    req_user, err := identifyUser(reqpath)
    if err != nil {
        return fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }
    if !isAuthorizedToAdmin(req_user, globals.Administrators) {
        return newHttpError(http.StatusForbidden, fmt.Errorf("user %q is not authorized to delete a project", req_user))
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
            return newHttpError(http.StatusBadRequest, fmt.Errorf("failed to parse JSON from %q; %w", reqpath, err))
        }

        err = isMissingOrBadName(incoming.Project)
        if err != nil {
            return newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'project' property in %q; %w", reqpath, err))
        }
    }

    rlock, err := lockDirectoryExclusive(globals, globals.Registry, ctx)
    if err != nil {
        return fmt.Errorf("failed to lock the registry; %w", err)
    }
    defer rlock.Unlock(globals)

    project_dir := filepath.Join(globals.Registry, *(incoming.Project))
    _, err = os.Stat(project_dir)
    if err != nil {
        if errors.Is(err, os.ErrNotExist) {
            return nil
        } else {
            return fmt.Errorf("failed to stat project directory %q; %w", project_dir, err)
        }
    }

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

func deleteAssetHandler(reqpath string, globals *globalConfiguration, ctx context.Context) error {
    req_user, err := identifyUser(reqpath)
    if err != nil {
        return fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }
    if !isAuthorizedToAdmin(req_user, globals.Administrators) {
        return newHttpError(http.StatusForbidden, fmt.Errorf("user %q is not authorized to delete a project", req_user))
    }

    incoming := struct {
        Project *string `json:"project"`
        Asset *string `json:"asset"`
        Force *bool `json:"force"`
    }{}
    {
        handle, err := os.ReadFile(reqpath)
        if err != nil {
            return fmt.Errorf("failed to read %q; %w", reqpath, err)
        }

        err = json.Unmarshal(handle, &incoming)
        if err != nil {
            return newHttpError(http.StatusBadRequest, fmt.Errorf("failed to parse JSON from %q; %w", reqpath, err))
        }

        err = isMissingOrBadName(incoming.Project)
        if err != nil {
            return newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'project' property in %q; %w", reqpath, err))
        }

        err = isMissingOrBadName(incoming.Asset)
        if err != nil {
            return newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'asset' property in %q; %w", reqpath, err))
        }
    }

    force_deletion := incoming.Force != nil && *(incoming.Force)

    rlock, err := lockDirectoryShared(globals, globals.Registry, ctx)
    if err != nil {
        return fmt.Errorf("failed to lock the registry %q; %w", globals.Registry, err)
    }
    defer rlock.Unlock(globals)

    project_dir := filepath.Join(globals.Registry, *(incoming.Project))
    _, err = os.Stat(project_dir)
    if err != nil {
        if errors.Is(err, os.ErrNotExist) {
            return nil
        } else {
            return fmt.Errorf("failed to stat project directory %q; %w", project_dir, err)
        }
    }

    plock, err := lockDirectoryExclusive(globals, project_dir, ctx)
    if err != nil {
        return fmt.Errorf("failed to lock the project directory %q; %w", project_dir, err)
    }
    defer plock.Unlock(globals)

    asset_dir := filepath.Join(project_dir, *(incoming.Asset))
    _, err = os.Stat(asset_dir)
    if err != nil {
        if errors.Is(err, os.ErrNotExist) {
            return nil
        } else {
            return fmt.Errorf("failed to stat asset directory %q; %w", asset_dir, err)
        }
    }

    asset_usage, asset_usage_err := computeAssetUsage(asset_dir)
    if asset_usage_err != nil && !force_deletion {
        return fmt.Errorf("failed to compute usage for %s; %v", asset_dir, asset_usage_err)
    }

    err = os.RemoveAll(asset_dir)
    if err != nil {
        return fmt.Errorf("failed to delete %s; %v", asset_dir, err)
    }

    if asset_usage_err == nil {
        err := editUsage(globals, project_dir, -asset_usage, ctx)
        if err != nil {
            return fmt.Errorf("failed to update usage for %s; %v", project_dir, err)
        }
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

func deleteVersionHandler(reqpath string, globals *globalConfiguration, ctx context.Context) error {
    req_user, err := identifyUser(reqpath)
    if err != nil {
        return fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }
    if !isAuthorizedToAdmin(req_user, globals.Administrators) {
        return newHttpError(http.StatusForbidden, fmt.Errorf("user %q is not authorized to delete a project", req_user))
    }

    incoming := struct {
        Project *string `json:"project"`
        Asset *string `json:"asset"`
        Version *string `json:"version"`
        Force *bool `json:"force"`
    }{}
    {
        handle, err := os.ReadFile(reqpath)
        if err != nil {
            return fmt.Errorf("failed to read %q; %w", reqpath, err)
        }

        err = json.Unmarshal(handle, &incoming)
        if err != nil {
            return newHttpError(http.StatusBadRequest, fmt.Errorf("failed to parse JSON from %q; %w", reqpath, err))
        }

        err = isMissingOrBadName(incoming.Project)
        if err != nil {
            return newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'project' property in %q; %w", reqpath, err))
        }
        err = isMissingOrBadName(incoming.Asset)
        if err != nil {
            return newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'asset' property in %q; %w", reqpath, err))
        }
        err = isMissingOrBadName(incoming.Version)
        if err != nil {
            return newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'version' property in %q; %w", reqpath, err))
        }
    }

    force_deletion := incoming.Force != nil && *(incoming.Force)

    rlock, err := lockDirectoryShared(globals, globals.Registry, ctx)
    if err != nil {
        return fmt.Errorf("failed to lock the registry %q; %w", globals.Registry, err)
    }
    defer rlock.Unlock(globals)

    project_dir := filepath.Join(globals.Registry, *(incoming.Project))
    _, err = os.Stat(project_dir)
    if err != nil {
        if errors.Is(err, os.ErrNotExist) {
            return nil
        } else {
            return fmt.Errorf("failed to stat project directory %q; %w", project_dir, err)
        }
    }

    plock, err := lockDirectoryShared(globals, project_dir, ctx)
    if err != nil {
        return fmt.Errorf("failed to lock project directory %q; %w", project_dir, err)
    }
    defer plock.Unlock(globals)

    asset_dir := filepath.Join(project_dir, *(incoming.Asset))
    _, err = os.Stat(asset_dir)
    if err != nil {
        if errors.Is(err, os.ErrNotExist) {
            return nil
        } else {
            return fmt.Errorf("failed to stat asset directory %q; %w", asset_dir, err)
        }
    }

    alock, err := lockDirectoryExclusive(globals, asset_dir, ctx)
    if err != nil {
        return fmt.Errorf("failed to lock asset directory %q; %w", asset_dir, err)
    }
    defer alock.Unlock(globals)

    version_dir := filepath.Join(asset_dir, *(incoming.Version))
    _, err = os.Stat(version_dir)
    if err != nil {
        if errors.Is(err, os.ErrNotExist) {
            return nil
        } else {
            return fmt.Errorf("failed to stat version directory %q; %w", version_dir, err)
        }
    }

    version_usage, version_usage_err := computeVersionUsage(version_dir)
    if version_usage_err != nil && !force_deletion {
        return fmt.Errorf("failed to compute usage for %s; %v", version_dir, version_usage_err)
    }

    summ, summ_err := readSummary(version_dir)
    if summ_err != nil && !force_deletion {
        return fmt.Errorf("failed to read summary for %s; %v", version_dir, summ_err)
    }

    err = os.RemoveAll(version_dir)
    if err != nil {
        return fmt.Errorf("failed to delete %s; %v", asset_dir, err)
    }

    if version_usage_err == nil {
        err := editUsage(globals, project_dir, -version_usage, ctx)
        if err != nil {
            return err
        }
    }

    if summ_err == nil && (summ.OnProbation == nil || !(*summ.OnProbation)) {
        // Only need to make a log if the version is non-probational.
        prev, err := readLatest(asset_dir)
        was_latest := false
        if err == nil {
            was_latest = (prev.Version == *(incoming.Version))
        } else if !errors.Is(err, os.ErrNotExist) {
            return fmt.Errorf("failed to read the latest version for %s; %v", asset_dir, err)
        }

        payload := map[string]interface{} { 
            "type": "delete-version", 
            "project": *(incoming.Project),
            "asset": *(incoming.Asset),
            "version": *(incoming.Version),
            "latest": was_latest,
        }

        err = dumpLog(globals.Registry, &payload)
        if err != nil {
            return fmt.Errorf("failed to create log for version deletion; %w", err)
        }

        // Also refreshing the latest version.
        _, latest_err := refreshLatest(asset_dir)
        if latest_err != nil && !force_deletion {
            return fmt.Errorf("failed to update the latest version for %s; %v", asset_dir, latest_err)
        }
    }

    return nil
}
