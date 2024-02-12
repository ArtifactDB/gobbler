package main

import (
    "os"
    "encoding/json"
    "fmt"
    "path/filepath"
    "time"
)

func baseProbationHandler(reqpath, registry string, administrators []string, approve bool) error {
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

    username, err := identifyUser(reqpath)
    if err != nil {
        return fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }

    project := *(incoming.Project)
    project_dir := filepath.Join(registry, project)
    lock_path := filepath.Join(project_dir, lockFileName)
    handle, err := lock(lock_path, 1000 * time.Second)
    if err != nil {
        return fmt.Errorf("failed to lock project directory %q; %w", project_dir, err)
    }
    defer unlock(handle)

    existing, err := readPermissions(project_dir)
    if err != nil {
        return fmt.Errorf("failed to read permissions for %q; %w", project_dir, err)
    }
    if !isAuthorizedToMaintain(username, administrators, existing.Owners) {
        return fmt.Errorf("user %q is not authorized to modify probation status in %q", username, project_dir)
    }

    version_dir := filepath.Join(project_dir, *(incoming.Asset), *(incoming.Version))
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

    } else {
        freed, err := computeUsage(version_dir, true)
        if err != nil {
            return fmt.Errorf("failed to compute usage for %q; %w", version_dir, err)
        }

        err = os.RemoveAll(version_dir)
        if err != nil {
            return fmt.Errorf("failed to delete %q; %w", version_dir, err)
        }

        usage, err := readUsage(project_dir)
        if err != nil {
            return fmt.Errorf("failed to read the usage statistics for %q; %w", project_dir, err)
        }

        usage.Total -= freed
        if usage.Total < 0 { // just in case.
            usage.Total = 0
        }

        usage_path := filepath.Join(project_dir, usageFileName)
        err = dumpJson(usage_path, &usage)
        if err != nil {
            return fmt.Errorf("failed to update project usage at %q; %w", usage_path, err)
        }
    }

    return nil
}

func approveProbationHandler(reqpath string, registry string, administrators []string) error {
    return baseProbationHandler(reqpath, registry, administrators, true)
}

func rejectProbationHandler(reqpath string, registry string, administrators []string) error {
    return baseProbationHandler(reqpath, registry, administrators, false)
}
