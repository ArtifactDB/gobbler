package main

import (
    "os"
    "encoding/json"
    "fmt"
    "io/fs"
    "strings"
    "path/filepath"
    "time"
)

type UsageMetadata struct {
    Total int64 `json:"total"`
}

const UsageFileName = "..usage"

func ReadUsage(path string) (*UsageMetadata, error) {
    usage_path := filepath.Join(path, UsageFileName)

    usage_raw, err := os.ReadFile(usage_path)
    if err != nil {
        return nil, fmt.Errorf("failed to read '" + usage_path + "'; %w", err)
    }

    var output UsageMetadata
    err = json.Unmarshal(usage_raw, &output)
    if err != nil {
        return nil, fmt.Errorf("failed to parse JSON in '" + usage_path + "'; %w", err)
    }

    return &output, nil
}

func ComputeUsage(dir string, skip_symlinks bool) (int64, error) {
    var total int64
    total = 0

    err := filepath.WalkDir(dir, func(path string, info fs.DirEntry, err error) error {
        if err != nil {
            return fmt.Errorf("failed to walk into %q; %w", path, err)
        }

        // Skipping internal files.
        base := filepath.Base(path)
        if strings.HasPrefix(base, "..") {
            return nil
        }

        restat, err := info.Info()
        if err != nil {
            return fmt.Errorf("failed to stat %q; %w", path, err)
        }

        if restat.Mode() & os.ModeSymlink == os.ModeSymlink {
            more_info, err := os.Stat(path)
            if err != nil {
                return fmt.Errorf("failed to stat target of link %q; %w", path, err)
            }
            if more_info.IsDir() {
                return fmt.Errorf("symlinks to directories are not supported (%q); %w", path, err)
            }
            if skip_symlinks {
                return nil
            } 
            total += more_info.Size()
        } else {
            if !info.IsDir() {
                total += restat.Size()
            }
        }

        return nil
    })

    return total, err
}

func refreshUsageHandler(reqpath, registry string, administrators []string) error {
    source_user, err := IdentifyUser(reqpath)
    if err != nil {
        return fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }

    if !IsAuthorizedToAdmin(source_user, administrators) {
        return fmt.Errorf("user %q is not authorized to refreseh the latest version (%q)", source_user, reqpath)
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
    }

    project_dir := filepath.Join(registry, *(incoming.Project))
    lock_path := filepath.Join(project_dir, LockFileName)
    handle, err := Lock(lock_path, 1000 * time.Second)
    if err != nil {
        return fmt.Errorf("failed to lock the project directory %q; %w", project_dir, err)
    }
    defer Unlock(handle)

    new_usage, err := ComputeUsage(project_dir, true)
    if err != nil {
        return fmt.Errorf("failed to compute usage for %q; %w", *(incoming.Project), err)
    }

    usage_path := filepath.Join(project_dir, UsageFileName)
    usage_meta := UsageMetadata{ Total: new_usage }
    err = dumpJson(usage_path, &usage_meta)
    if err != nil {
        return fmt.Errorf("failed to write new usage for %q; %w", *(incoming.Project), err)
    }

    return nil
}
