package main

import (
    "os"
    "encoding/json"
    "fmt"
    "path/filepath"
    "time"
    "errors"
    "net/http"
    "context"
    "sync"
)

func rejectProbation(project_dir, version_dir string, force_deletion bool, globals *globalConfiguration, ctx context.Context) error {
    // Assumes that we have an exclusive asset-level lock. 
    version_usage, version_usage_err := computeVersionUsage(version_dir)
    if version_usage_err != nil && !force_deletion {
        return fmt.Errorf("failed to compute usage for %q; %w", version_dir, version_usage_err)
    }

    err := os.RemoveAll(version_dir)
    if err != nil {
        return fmt.Errorf("failed to delete %q; %w", version_dir, err)
    }

    if version_usage_err == nil {
        err := editUsage(project_dir, -version_usage, globals, ctx)
        if err != nil {
            return fmt.Errorf("failed to update usage for project directory %q; %w", project_dir, err)
        }
    }

    return nil
}

func baseProbationHandler(reqpath string, approve bool, globals *globalConfiguration, ctx context.Context) error {
    incoming := struct {
        Project *string `json:"project"`
        Asset *string `json:"asset"`
        Version *string `json:"version"`
        Force *bool `json:"force"`
        Spoof *string `json:"spoof"`
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

    username, err := identifySpoofedUser(reqpath, incoming.Spoof, globals.SpoofPermissions)
    if err != nil {
        return fmt.Errorf("failed to identify user; %w", err)
    }

    rlock, err := lockDirectoryShared(globals.Registry, globals, ctx)
    if err != nil {
        return fmt.Errorf("failed to lock the registry %q; %w", globals.Registry, err)
    }
    defer rlock.Unlock(globals)

    rnnlock, err := lockDirectoryNewDirShared(globals.Registry, globals, ctx)
    if err != nil {
        return fmt.Errorf("failed to lock the registry %q; %w", globals.Registry, err)
    }
    defer rnnlock.Unlock(globals)

    project := *(incoming.Project)
    project_dir := filepath.Join(globals.Registry, project)
    if err := checkProjectExists(project_dir, project); err != nil {
        return err
    }
    rnnlock.Unlock(globals) // no need for this lock once we determine that the project directory exists.

    plock, err := lockDirectoryShared(project_dir, globals, ctx)
    if err != nil {
        return fmt.Errorf("failed to lock project directory %q; %w", project_dir, err)
    }
    defer plock.Unlock(globals)

    existing, err := readPermissions(project_dir)
    if err != nil {
        return fmt.Errorf("failed to read permissions for %q; %w", project_dir, err)
    }
    if !isAuthorizedToMaintain(username, globals.Administrators, existing.Owners) {
        return newHttpError(http.StatusForbidden, fmt.Errorf("user %q is not authorized to modify probation status in %q", username, project_dir))
    }

    pnnlock, err := lockDirectoryNewDirShared(project_dir, globals, ctx)
    if err != nil {
        return fmt.Errorf("failed to lock project directory %q; %w", project_dir, err)
    }
    defer pnnlock.Unlock(globals)

    asset := *(incoming.Asset)
    asset_dir := filepath.Join(project_dir, asset)
    if err := checkAssetExists(asset_dir, asset, project); err != nil {
        return err
    }
    pnnlock.Unlock(globals) // no need for this lock once we determine that the asset directory exists.

    alock, err := lockDirectoryExclusive(asset_dir, globals, ctx)
    if err != nil {
        return fmt.Errorf("failed to lock asset directory %q; %w", asset_dir, err)
    }
    defer alock.Unlock(globals)

    version := *(incoming.Version)
    version_dir := filepath.Join(asset_dir, version)
    if err := checkVersionExists(version_dir, version, asset, project); err != nil {
        return err
    }

    summ, err := readSummary(version_dir)
    if err != nil {
        return fmt.Errorf("failed to read the version summary at %q; %w", version_dir, err)
    }
    if !summ.IsProbational() {
        return fmt.Errorf("version directory at %q is not on probation", version_dir)
    }

    if approve {
        summ.OnProbation = nil
        summary_path := filepath.Join(version_dir, summaryFileName)
        err = dumpJson(summary_path, &summ)
        if err != nil {
            return fmt.Errorf("failed to update the version summary at %q; %w", summary_path, err)
        }

        latest, err := readLatest(asset_dir)
        overwrite_latest := false
        if err == nil {
            latest_version := filepath.Join(asset_dir, latest.Version)
            latest_summ, err := readSummary(latest_version)
            if err != nil {
                return fmt.Errorf("failed to read the latest version summary for %q; %w", latest_version, err)
            }

            finish_time, err := time.Parse(time.RFC3339, summ.UploadFinish)
            if err != nil {
                return fmt.Errorf("failed to parse the upload finish time at %q; %w", summary_path, err)
            }
            latest_time, err := time.Parse(time.RFC3339, latest_summ.UploadFinish)
            if err != nil {
                return fmt.Errorf("failed to read the latest version's upload finish time from %q; %w", latest_version, err)
            }
            overwrite_latest = finish_time.After(latest_time) 
        } else if errors.Is(err, os.ErrNotExist) {
            overwrite_latest = true
            latest = &latestMetadata{}
        } else {
            return fmt.Errorf("failed to read the latest version for %s; %w", asset_dir, err)
        }

        if overwrite_latest {
            latest_path := filepath.Join(asset_dir, latestFileName)
            latest.Version = *(incoming.Version)
            err := dumpJson(latest_path, latest)
            if err != nil {
                return fmt.Errorf("failed to update the latest version at %q; %w", latest_path, err)
            }
        }

        // Adding a log.
        log_info := map[string]interface{} {
            "type": "add-version",
            "project": project,
            "asset": asset,
            "version": version,
            "latest": overwrite_latest,
        }
        err = dumpLog(globals.Registry, &log_info)
        if err != nil {
            return fmt.Errorf("failed to save log file; %w", err)
        }

    } else {
        force_deletion := incoming.Force != nil && *(incoming.Force)
        err = rejectProbation(project_dir, version_dir, force_deletion, globals, ctx)
        if err != nil {
            return err
        }
    }

    return nil
}

func approveProbationHandler(reqpath string, globals *globalConfiguration, ctx context.Context) error {
    return baseProbationHandler(reqpath, true, globals, ctx)
}

func rejectProbationHandler(reqpath string, globals *globalConfiguration, ctx context.Context) error {
    return baseProbationHandler(reqpath, false, globals, ctx)
}

func purgeOldProbationalVersions(globals *globalConfiguration, expiry time.Duration) []error {
    ctx := context.Background()

    rlock, err := lockDirectoryShared(globals.Registry, globals, ctx)
    if err != nil {
        return []error{ fmt.Errorf("failed to lock the registry %q; %w", globals.Registry, err) }
    }
    defer rlock.Unlock(globals)

    rnnlock, err := lockDirectoryShared(globals.Registry, globals, ctx)
    if err != nil {
        return []error{ fmt.Errorf("failed to lock the registry %q; %w", globals.Registry, err) }
    }
    defer rnnlock.Unlock(globals)

    projects, err := listUserDirectories(globals.Registry)
    if err != nil {
        return []error{ fmt.Errorf("failed to list projects in registry; %w", err) }
    }
    rnnlock.Unlock(globals) // no need for this lock once we obtain a list of the current directories.

    all_errors := []error{}
    for _, project := range projects {
        project_dir := filepath.Join(globals.Registry, project)
        cur_errors := purgeOldProbationalVersionsForProject(project_dir, expiry, globals, ctx)
        all_errors = append(all_errors, cur_errors...)
    }

    return all_errors
}

func purgeOldProbationalVersionsForProject(project_dir string, expiry time.Duration, globals *globalConfiguration, ctx context.Context) []error {
    plock, err := lockDirectoryShared(project_dir, globals, ctx)
    if err != nil {
        return []error{ fmt.Errorf("failed to lock project directory %q; %w", project_dir, err) }
    }
    defer plock.Unlock(globals)

    pnnlock, err := lockDirectoryNewDirShared(project_dir, globals, ctx)
    if err != nil {
        return []error{ fmt.Errorf("failed to lock project directory %q; %w", project_dir, err) }
    }
    defer pnnlock.Unlock(globals)

    assets, err := listUserDirectories(project_dir)
    if err != nil {
        return []error{ fmt.Errorf("failed to list assets in project directory %q; %w", project_dir, err) }
    }
    pnnlock.Unlock(globals) // no need for this lock once we obtain a list of the current directories.

    all_errors := []error{}
    for _, asset := range assets {
        asset_dir := filepath.Join(project_dir, asset)
        cur_errors := purgeOldProbationalVersionsForAsset(project_dir, asset_dir, expiry, globals, ctx)
        all_errors = append(all_errors, cur_errors...)
    }

    return all_errors
}

func purgeOldProbationalVersionsForAsset(project_dir string, asset_dir string, expiry time.Duration, globals *globalConfiguration, ctx context.Context) []error {
    alock, err := lockDirectoryExclusive(asset_dir, globals, ctx)
    if err != nil {
        return []error{ fmt.Errorf("failed to lock asset directory %q; %w", asset_dir, err) }
    }
    defer alock.Unlock(globals)

    versions, err := listUserDirectories(asset_dir)
    if err != nil {
        return []error{ fmt.Errorf("failed to list versions in asset directory %q; %w", asset_dir, err) }
    }

    // We parallelize across versions rather than across assets, as we don't want too many goroutines contesting/holding asset locks;
    // this could unnecessarily block other endpoints from proceeding.
    all_errors := []error{}
    var error_lock sync.Mutex
    safeAddError := func(err error) {
        error_lock.Lock()
        defer error_lock.Unlock()
        all_errors = append(all_errors, err)
    }

    var wg sync.WaitGroup
    defer wg.Wait() // don't release the directory lock while goroutines are still operating inside!

    for _, version := range versions {
        handle := globals.ConcurrencyThrottle.Wait()
        wg.Add(1)
        go func() {
            defer globals.ConcurrencyThrottle.Release(handle)
            defer wg.Done()

            version_dir := filepath.Join(asset_dir, version)
            summ, err := readSummary(version_dir)
            if err != nil {
                safeAddError(fmt.Errorf("failed to open summary file at %s; %w", version_dir, err))
                return
            }
            if summ.OnProbation == nil || !*(summ.OnProbation) {
                return
            }

            as_time, err := time.Parse(time.RFC3339, summ.UploadFinish)
            if err != nil {
                safeAddError(fmt.Errorf("failed to open parse upload time for summary file at %s; %w", version_dir, err))
                return
            }

            if time.Now().Sub(as_time) > expiry {
                err := rejectProbation(project_dir, version_dir, false, globals, ctx)
                if err != nil {
                    safeAddError(err)
                }
            }
        }()
    }

    wg.Wait()
    return all_errors
}
