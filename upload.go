package main

import (
    "fmt"
    "time"
    "path/filepath"
    "os"
    "encoding/json"
    "errors"
    "net/http"
)

type uploadRequest struct {
    Source *string `json:"source"`
    Project *string `json:"project"`
    Asset *string `json:"asset"`
    Version *string `json:"version"`
    OnProbation *bool `json:"on_probation"`
    User string `json:"-"`
}

func uploadPreflight(reqpath string) (*uploadRequest, error) {
    handle, err := os.ReadFile(reqpath)
    if err != nil {
        return nil, fmt.Errorf("failed to read %q; %w", reqpath, err)
    }

    req_user, err := identifyUser(reqpath)
    if err != nil {
        return nil, fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }

    request := uploadRequest{}
    err = json.Unmarshal(handle, &request)
    if err != nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("failed to parse JSON from %q; %w", reqpath, err))
    }

    if request.Source == nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("expected a 'source' property in %q; %w", reqpath, err))
    }
    source := *(request.Source)
    if source != filepath.Base(source) {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("expected 'source' to be in the same directory as %q", reqpath))
    }
    source = filepath.Join(filepath.Dir(reqpath), source)
    source_user, err := identifyUser(source)
    if err != nil {
        return nil, fmt.Errorf("failed to find owner of %q; %w", source, err)
    }
    if source_user != req_user {
        return nil, newHttpError(http.StatusForbidden, fmt.Errorf("requesting user must be the same as the owner of the 'source' directory (%s vs %s)", source_user, req_user))
    }
    request.Source = &source

    if request.Project == nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("expected a 'project' property in %q", reqpath))
    }
    project := *(request.Project)
    err = isBadName(project)
    if err != nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("invalid project name %q; %w", project, err))
    }

    if request.Asset == nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("expected an 'asset' property in %q", reqpath))
    }
    asset := *(request.Asset)
    err = isBadName(asset)
    if err != nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("invalid asset name %q; %w", asset, err))
    }

    if request.Version == nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("expected a 'version' property in %q", reqpath))
    }
    version := *(request.Version)
    err = isBadName(version)
    if err != nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("invalid version name %q; %w", version, err))
    }

    request.User = req_user
    return &request, nil
}

func uploadHandler(reqpath string, globals *globalConfiguration) error {
    upload_start := time.Now()

    request, err := uploadPreflight(reqpath)
    if err != nil {
        return err
    }

    req_user := request.User
    on_probation := request.OnProbation != nil && *(request.OnProbation)

    // Configuring the project; we apply a lock to the project to avoid concurrent changes.
    project := *(request.Project)
    project_dir := filepath.Join(globals.Registry, project)
    err = globals.Locks.LockDirectory(project_dir, 10 * time.Second)
    if err != nil {
        return fmt.Errorf("failed to acquire the lock on %q; %w", project_dir, err)
    }
    defer globals.Locks.Unlock(project_dir)

    perms, err := readPermissions(project_dir)
    if err != nil {
        return fmt.Errorf("failed to read permissions for %q; %w", project, err)
    }

    // Check if this upload is authorized via the global write permissions. 
    global_write_new_asset, err := prepareGlobalWriteNewAsset(req_user, perms, *(request.Asset), project_dir)
    if err != nil {
        return fmt.Errorf("failed to update the permissions for a new asset with global write; %w", err)
    }

    if !global_write_new_asset {
        ok, trusted := isAuthorizedToUpload(req_user, globals.Administrators, perms, request.Asset, request.Version)
        if !ok {
            return newHttpError(http.StatusForbidden, fmt.Errorf("user '" + req_user + "' is not authorized to upload to '" + project + "'"))
        }
        if !trusted {
            on_probation = true
        }
    }

    // Configuring the asset and version.
    asset := *(request.Asset)
    asset_dir := filepath.Join(project_dir, asset)
    if _, err := os.Stat(asset_dir); errors.Is(err, os.ErrNotExist) {
        err = os.Mkdir(asset_dir, 0755)
        if err != nil {
            return fmt.Errorf("failed to create a new asset directory inside %q; %w", asset_dir, err)
        }
    }

    version := *(request.Version)
    version_dir := filepath.Join(asset_dir, version)
    if _, err := os.Stat(version_dir); err == nil {
        return newHttpError(http.StatusBadRequest, fmt.Errorf("version %q already exists in %q", version, asset_dir))
    }

    err = os.Mkdir(version_dir, 0755)
    if err != nil {
        return fmt.Errorf("failed to create a new version directory at %q; %w", version_dir, err)
    }

    // Now transferring all the files. This involves setting up an abort loop
    // to remove failed uploads from the globals.Registry, lest they clutter things up.
    has_failed := true
    defer func() {
        if has_failed {
            os.RemoveAll(filepath.Join(globals.Registry, project, asset, version))
        }
    }()

    source := *(request.Source)
    err = Transfer(source, globals.Registry, project, asset, version)
    if err != nil {
        return fmt.Errorf("failed to transfer files from %q; %w", source, err)
    }

    // Writing out the various pieces of metadata. This should be, in theory,
    // the 'no-throw' section as no user-supplied values are involved here.
    upload_finish := time.Now()
    {
        summary := summaryMetadata {
            UploadUserId: req_user,
            UploadStart: upload_start.Format(time.RFC3339),
            UploadFinish: upload_finish.Format(time.RFC3339),
        }
        if on_probation {
            summary.OnProbation = &on_probation
        }

        summary_path := filepath.Join(version_dir, summaryFileName)
        err := dumpJson(summary_path, &summary)
        if err != nil {
            return fmt.Errorf("failed to save summary for %q; %w", asset_dir, err)
        }
    }

    {
        extra, err := computeUsage(version_dir, true)
        if err != nil {
            return fmt.Errorf("failed to compute usage for the new version at %q; %w", version_dir, err)
        }

        usage, err := readUsage(project_dir)
        if err != nil {
            return fmt.Errorf("failed to read existing usage for project %q; %w", project, err)
        }
        usage.Total += extra

        // Try to do this write later to reduce the chance of an error
        // triggering an abort after the usage total has been updated.
        usage_path := filepath.Join(project_dir, usageFileName)
        err = dumpJson(usage_path, &usage)
        if err != nil {
            return fmt.Errorf("failed to save usage for %q; %w", project_dir, err)
        }
    }

    if !on_probation {
        // Doing this as late as possible to reduce the chances of an error
        // triggering an abort _after_ the latest version has been updated.
        // I suppose we could try to reset to the previous value; but if the
        // writes failed there's no guarantee that a reset would work either.
        latest := latestMetadata { Version: version }
        latest_path := filepath.Join(asset_dir, latestFileName)
        err := dumpJson(latest_path, &latest)
        if err != nil {
            return fmt.Errorf("failed to save latest version for %q; %w", asset_dir, err)
        }

        // Adding a log.
        log_info := map[string]interface{} {
            "type": "add-version",
            "project": project,
            "asset": asset,
            "version": version,
            "latest": true,
        }
        err = dumpLog(globals.Registry, log_info)
        if err != nil {
            return fmt.Errorf("failed to save log file; %w", err)
        }
    }

    has_failed = false
    return nil
}
