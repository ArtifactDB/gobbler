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
    Consume *bool `json:"consume"`
    IgnoreDot *bool `json:"ignore_dot"`
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
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("expected 'source' to be a name, not a path, in %q", reqpath))
    }

    // Forbid references to files, symlinks within the staging directory.
    source_name := source
    source = filepath.Join(filepath.Dir(reqpath), source)
    source_info, err := os.Lstat(source)
    if err != nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("failed to stat %q in the staging directory; %w", source_name, err))
    }
    if !source_info.IsDir() {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("expected %q to be a directory", source_name))
    }

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

    rlock, err := lockDirectoryShared(globals, globals.Registry)
    if err != nil {
        return fmt.Errorf("failed to lock the registry %q; %w", globals.Registry, err)
    }
    defer rlock.Unlock()

    project := *(request.Project)
    project_dir := filepath.Join(globals.Registry, project)
    if err := checkProjectExists(project_dir, project); err != nil {
        return err
    }

    asset := *(request.Asset)
    asset_dir := filepath.Join(project_dir, asset)

    err = func() error {
        // Acquiring an exclusive lock for safety during asset directory creation and reading/setting asset-level permissions.
        plock, err := lockDirectoryExclusive(globals, project_dir)
        if err != nil {
            return fmt.Errorf("failed to lock project directory %q; %w", project_dir, err)
        }
        defer plock.Unlock()

        perms, err := readPermissions(project_dir)
        if err != nil {
            return fmt.Errorf("failed to read permissions for %q; %w", project, err)
        }

        asset_exists := false
        _, err = os.Stat(asset_dir)
        if err == nil {
            asset_exists = true
        } else if !errors.Is(err, os.ErrNotExist) {
            return fmt.Errorf("failed to stat asset directory %q; %w", asset_dir, err)
        }

        use_global_write := perms.GlobalWrite != nil && *(perms.GlobalWrite) && !asset_exists
        if !use_global_write {
            asset_perms, err := addAssetPermissionsForUpload(perms, asset_dir, asset)
            if err != nil {
                return fmt.Errorf("failed to read permissions for asset %q in %q; %w", asset, project, err)
            }

            ok, trusted := isAuthorizedToUpload(req_user, globals.Administrators, asset_perms, request.Asset, request.Version)
            if !ok {
                return newHttpError(http.StatusForbidden, fmt.Errorf("user '" + req_user + "' is not authorized to upload to '" + project + "'"))
            }
            if !trusted {
                on_probation = true
            }
        }

        if !asset_exists {
            err = os.Mkdir(asset_dir, 0755)
            if err != nil {
                return fmt.Errorf("failed to create a new asset directory inside %q; %w", asset_dir, err)
            }
        }

        if use_global_write { // adding asset-level permissions.
            asset_permissions := &permissionsMetadata{ Owners: []string{ req_user } }
            perm_path := filepath.Join(asset_dir, permissionsFileName)
            err := dumpJson(perm_path, asset_permissions)
            if err != nil {
                return fmt.Errorf("failed to create new permissions for asset %q in %q; %w", asset, project, err)
            }
        }

        return nil
    }()
    if err != nil {
        return err
    }

    version := *(request.Version)
    version_dir := filepath.Join(asset_dir, version)

    // Set up an abort loop to remove failed uploads from the globals.Registry, lest they clutter things up.
    has_failed := true
    defer func() {
        if has_failed {
            os.RemoveAll(filepath.Join(globals.Registry, project, asset, version))
        }
    }()

    var extra_usage int64
    err = func() error {
        // Now switching to a shared project-level lock to improve parallelism.
        plock, err := lockDirectoryShared(globals, project_dir)
        if err != nil {
            return fmt.Errorf("failed to re-lock project directory %q; %w", project_dir, err)
        }
        defer plock.Unlock()

        // Re-checking that the asset directory still exists, in case some other handler deleted it after lock re-acquisition.
        if _, err := os.Stat(asset_dir); err != nil {
            return fmt.Errorf("asset directory %q no longer exists; %w", project_dir, err)
        }

        alock, err := lockDirectoryExclusive(globals, asset_dir)
        if err != nil {
            return fmt.Errorf("failed to lock asset directory %q; %w", asset_dir, err)
        }
        defer alock.Unlock()

        if _, err := os.Stat(version_dir); err == nil {
            return newHttpError(http.StatusBadRequest, fmt.Errorf("version %q already exists in %q", version, asset_dir))
        }

        err = os.Mkdir(version_dir, 0755)
        if err != nil {
            return fmt.Errorf("failed to create a new version directory at %q; %w", version_dir, err)
        }

        source := *(request.Source)
        err = transferDirectory(
            source,
            globals.Registry,
            project,
            asset,
            version,
            transferDirectoryOptions{
                TryMove: (request.Consume != nil && *(request.Consume)),
                IgnoreDot: (request.IgnoreDot != nil && *(request.IgnoreDot)),
                LinkWhitelist: globals.LinkWhitelist,
            },
        )

        if err != nil {
            return fmt.Errorf("failed to transfer files from %q; %w", source, err)
        }

        upload_finish := time.Now()
        summary := summaryMetadata {
            UploadUserId: req_user,
            UploadStart: upload_start.Format(time.RFC3339),
            UploadFinish: upload_finish.Format(time.RFC3339),
        }
        if on_probation {
            summary.OnProbation = &on_probation
        }

        summary_path := filepath.Join(version_dir, summaryFileName)
        err = dumpJson(summary_path, &summary)
        if err != nil {
            return fmt.Errorf("failed to save summary for %q; %w", asset_dir, err)
        }

        if !on_probation {
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

        extra_usage, err = computeVersionUsage(version_dir)
        if err != nil {
            return fmt.Errorf("failed to compute usage for the new version at %q; %w", version_dir, err)
        }

        return nil
    }()
    if err != nil {
        return err
    }

    err = func() error {
        // Reacquire an exclusive project-level lock to edit the usage.
        // In theory, the just-uploaded version could be deleted by deleteAssetHandler() or rejectProbationHandler() before we get to this point.
        // If those handlers subtract the version's usage from the project's usage, the latter will be incorrect as we never added the former's usage.
        // This deficit is eventually corrected once we run through this section and add the version's usage back to the project's usage.
        plock, err := lockDirectoryExclusive(globals, project_dir)
        if err != nil {
            return fmt.Errorf("failed to re-lock project directory %q; %w", project_dir, err)
        }
        defer plock.Unlock()

        usage, err := readUsage(project_dir)
        if err != nil {
            return fmt.Errorf("failed to read existing usage for project %q; %w", project, err)
        }
        usage.Total += extra_usage

        usage_path := filepath.Join(project_dir, usageFileName)
        err = dumpJson(usage_path, &usage)
        if err != nil {
            return fmt.Errorf("failed to save usage for %q; %w", project_dir, err)
        }

        return nil
    }()
    if err != nil {
        return err
    }

    has_failed = false
    return nil
}
