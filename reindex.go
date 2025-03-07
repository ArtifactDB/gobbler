package main

import (
    "fmt"
    "time"
    "path/filepath"
    "os"
    "encoding/json"
    "net/http"
    "errors"
)

type reindexRequest struct {
    Project *string `json:"project"`
    Asset *string `json:"asset"`
    Version *string `json:"version"`
    User string `json:"-"`
}

func reindexPreflight(reqpath string) (*reindexRequest, error) {
    handle, err := os.ReadFile(reqpath)
    if err != nil {
        return nil, fmt.Errorf("failed to read %q; %w", reqpath, err)
    }

    req_user, err := identifyUser(reqpath)
    if err != nil {
        return nil, fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }

    request := reindexRequest{}
    err = json.Unmarshal(handle, &request)
    if err != nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("failed to parse JSON from %q; %w", reqpath, err))
    }

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

func reindexHandler(reqpath string, globals *globalConfiguration) error {
    request, err := reindexPreflight(reqpath)
    if err != nil {
        return err
    }

    // Configuring the project; we apply a lock to the project to avoid concurrent changes.
    project := *(request.Project)
    project_dir := filepath.Join(globals.Registry, project)
    if err := checkProjectExists(project_dir, project); err != nil {
        return err
    }

    err = lockProject(globals, project_dir, 10 * time.Second)
    if err != nil {
        return fmt.Errorf("failed to acquire the lock on %q; %w", project_dir, err)
    }
    defer unlockProject(globals, project_dir)

    // Check if this reindexing request is properly authorized. 
    perms, err := readPermissions(project_dir)
    if err != nil {
        return fmt.Errorf("failed to read permissions for %q; %w", project, err)
    }

    ok := isAuthorizedToMaintain(request.User, globals.Administrators, perms.Owners)
    if !ok {
        return newHttpError(http.StatusForbidden, fmt.Errorf("user '" + request.User + "' is not authorized to reindex '" + project + "'"))
    }

    // Configuring the asset and version.
    asset := *(request.Asset)
    asset_dir := filepath.Join(project_dir, asset)
    if err := checkAssetExists(asset_dir, asset, project); err != nil {
        return err
    }

    version := *(request.Version)
    version_dir := filepath.Join(asset_dir, version)
    if err := checkVersionExists(version_dir, version, asset, project); err != nil {
        return err
    }

    err = reindexDirectory(globals.Registry, project, asset, version, globals.LinkWhitelist)
    if err != nil {
        return fmt.Errorf("failed to reindex project; %w", err)
    }

    summ, err := readSummary(version_dir)
    if err != nil {
        return fmt.Errorf("failed to read the summary file at %q; %w", version_dir, err)
    }

    if summ.OnProbation == nil || !*(summ.OnProbation) {
        latest, err := readLatest(asset_dir)
        is_latest := false
        if err == nil {
            is_latest = latest.Version == version
        } else if !errors.Is(err, os.ErrNotExist) {
            return fmt.Errorf("failed to read latest version for %q; %w", asset_dir, err)
        }

        log_info := map[string]interface{} {
            "type": "reindex-version",
            "project": project,
            "asset": asset,
            "version": version,
            "latest": is_latest,
        }
        err = dumpLog(globals.Registry, log_info)
        if err != nil {
            return fmt.Errorf("failed to save log file; %w", err)
        }
    }

    return nil
}
