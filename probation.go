package main

import (
    "os"
    "encoding/json"
    "fmt"
    "path/filepath"
    "time"
    "errors"
    "net/http"
)

func rejectProbation(project_dir, version_dir string, force_deletion bool) error {
    version_usage, version_usage_err := computeVersionUsage(version_dir)
    if version_usage_err != nil && !force_deletion {
        return fmt.Errorf("failed to compute usage for %q; %w", version_dir, version_usage_err)
    }

    err := os.RemoveAll(version_dir)
    if err != nil {
        return fmt.Errorf("failed to delete %q; %w", version_dir, err)
    }

    if version_usage_err == nil {
        project_usage, err := readUsage(project_dir)
        if err != nil {
            return fmt.Errorf("failed to read the usage statistics for %q; %w", project_dir, err)
        }

        project_usage.Total -= version_usage
        if project_usage.Total < 0 { // just in case.
            project_usage.Total = 0
        }

        usage_path := filepath.Join(project_dir, usageFileName)
        err = dumpJson(usage_path, &project_usage)
        if err != nil {
            return fmt.Errorf("failed to update project usage at %q; %w", usage_path, err)
        }
    }

    return nil
}

func baseProbationHandler(reqpath string, globals *globalConfiguration, approve bool) error {
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

    username, err := identifyUser(reqpath)
    if err != nil {
        return fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }

    project := *(incoming.Project)
    project_dir := filepath.Join(globals.Registry, project)
    if err := checkProjectExists(project_dir, project); err != nil {
        return err
    }

    err = globals.Locks.LockDirectory(project_dir, 10 * time.Second)
    if err != nil {
        return fmt.Errorf("failed to lock project directory %q; %w", project_dir, err)
    }
    defer globals.Locks.Unlock(project_dir)

    existing, err := readPermissions(project_dir)
    if err != nil {
        return fmt.Errorf("failed to read permissions for %q; %w", project_dir, err)
    }
    if !isAuthorizedToMaintain(username, globals.Administrators, existing.Owners) {
        return newHttpError(http.StatusForbidden, fmt.Errorf("user %q is not authorized to modify probation status in %q", username, project_dir))
    }

    asset := *(incoming.Asset)
    asset_dir := filepath.Join(project_dir, asset)
    if err := checkAssetExists(asset_dir, asset, project); err != nil {
        return err
    }

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
        err := rejectProbation(project_dir, version_dir, force_deletion)
        if err != nil {
            return err
        }
    }

    return nil
}

func approveProbationHandler(reqpath string, globals *globalConfiguration) error {
    return baseProbationHandler(reqpath, globals, true)
}

func rejectProbationHandler(reqpath string, globals *globalConfiguration) error {
    return baseProbationHandler(reqpath, globals, false)
}

func purgeOldProbationalVersionsForProject(globals *globalConfiguration, project string, expiry time.Duration) []error {
    project_dir := filepath.Join(globals.Registry, project)

    err := globals.Locks.LockDirectory(project_dir, 10 * time.Second)
    if err != nil {
        return []error{ fmt.Errorf("failed to acquire the lock on %q; %w", project_dir, err) }
    }
    defer globals.Locks.Unlock(project_dir)

    assets, err := listUserDirectories(project_dir)
    if err != nil {
        return []error{ fmt.Errorf("failed to list assets for project %q; %w", project, err) }
    }

    now := time.Now()
    all_errors := []error{}
    for _, asset := range assets {
        asset_dir := filepath.Join(project_dir, asset)
        versions, err := listUserDirectories(asset_dir)
        if err != nil {
            all_errors = append(all_errors, fmt.Errorf("failed to list versions for asset %q in project %q; %w", asset, project, err))
            continue
        }

        for _, version := range versions {
            version_dir := filepath.Join(asset_dir, version)
            summ, err := readSummary(version_dir)
            if err != nil {
                all_errors = append(all_errors, fmt.Errorf("failed to open summary file at %s; %w", version_dir, err))
                continue
            }
            if summ.OnProbation == nil || !*(summ.OnProbation) {
                continue
            }
            as_time, err := time.Parse(time.RFC3339, summ.UploadFinish)
            if err != nil {
                all_errors = append(all_errors, fmt.Errorf("failed to open parse upload time for summary file at %s; %w", version_dir, err))
                continue
            }
            if now.Sub(as_time) > expiry {
                err := rejectProbation(project_dir, version_dir, false)
                if err != nil {
                    all_errors = append(all_errors, err)
                }
            }
        }
    }

    return all_errors
}

func purgeOldProbationalVersions(globals *globalConfiguration, expiry time.Duration) []error {
    projects, err := listUserDirectories(globals.Registry)
    if err != nil {
        return []error{ fmt.Errorf("failed to list projects in registry; %w", err) }
    }
    all_errors := []error{}
    for _, project := range projects {
        cur_errors := purgeOldProbationalVersionsForProject(globals, project, expiry)
        all_errors = append(all_errors, cur_errors...)
    }
    return all_errors
}
