package main

import (
    "fmt"
    "time"
    "path/filepath"
    "os"
    "encoding/json"
    "strconv"
    "strings"
    "errors"
    "regexp"
    "unicode"
)


func createProject(registry string, username string, project string, locks *pathLocks) error {
    err := isBadName(project)
    if err != nil {
        return "", false, fmt.Errorf("invalid project name; %w", err)
    }

    // Creating a new project from a pre-supplied name.
    project_dir := filepath.Join(registry, project_str)
    info, err := os.Stat(project_dir)
    if err == nil || errors.Is(err, os.ErrNotExist) {
        return "", false, fmt.Errorf("project %q already exists", project)
    }

    // No need to lock here, MkdirAll just no-ops if the directory already exists.
    err := os.MkdirAll(project_dir, 0755)

    locks.LockPath(project_dir)
    if err != nil {
        return nil, fmt.Errorf("failed to acquire the lock on %q; %w", project_dir, err)
    }
    defer locks.UnlockPath(project_dir)

    // Adding permissions.
    perms := permissionsMetadata{}
    if permissions != nil && permissions.Owners != nil {
        perms.Owners = permissions.Owners
    } else {
        perms.Owners = []string{ username }
    }
    if permissions != nil && permissions.Uploaders != nil {
        san, err := sanitizeUploaders(permissions.Uploaders)
        if err != nil {
            return fmt.Errorf("invalid 'permissions.uploaders' in the request details; %w", err)
        }
        perms.Uploaders = san
    } else {
        perms.Uploaders = []uploaderEntry{}
    }

    err := dumpJson(filepath.Join(dir, permissionsFileName), &perms)
    if err != nil {
        return fmt.Errorf("failed to write permissions for %q; %w", dir, err)
    }

    // Dumping a mock quota and usage file for consistency with gypsum.
    // Note that the quota isn't actually enforced yet.
    err = os.WriteFile(filepath.Join(dir, "..quota"), []byte("{ \"baseline\": 1000000000, \"growth_rate\": 1000000000, \"year\": " + strconv.Itoa(time.Now().Year()) + " }"), 0755)
    if err != nil {
        return fmt.Errorf("failed to write quota for '" + dir + "'; %w", err)
    }

    err = os.WriteFile(filepath.Join(dir, usageFileName), []byte("{ \"total\": 0 }"), 0755)
    if err != nil {
        return fmt.Errorf("failed to write usage for '" + dir + "'; %w", err)
    }

    return project_str, false, nil
}


