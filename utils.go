package main

import (
    "encoding/json"
    "os"
    "fmt"
    "errors"
    "strings"
    "time"
    "math/rand"
    "strconv"
    "path/filepath"
    "net/http"
)

type globalConfiguration struct {
    Registry string
    Administrators []string
    Locks pathLocks
    LinkWhitelist []string
    SpoofPermissions map[string]spoofPermissions
    ConcurrencyThrottle concurrencyThrottle
}

func newGlobalConfiguration(registry string, max_concurrency int) globalConfiguration {
    return globalConfiguration{ 
        Registry: registry, 
        Administrators: []string{},
        Locks: newPathLocks(),
        LinkWhitelist: []string{},
        SpoofPermissions: map[string]spoofPermissions{},
        ConcurrencyThrottle: newConcurrencyThrottle(max_concurrency),
    }
}

type httpError struct {
    Status int
    Reason error
}

func (r *httpError) Error() string {
    return r.Reason.Error()
}

func (r *httpError) Unwrap() error {
    return r.Reason
}

func newHttpError(status int, reason error) *httpError {
    return &httpError{ Status: status, Reason: reason }
}

func dumpJson(path string, content interface{}) error {
    // Using the save-and-rename paradigm to avoid clients picking up partial writes.
    temp, err := os.CreateTemp(filepath.Dir(path), ".temp*.json")
    if err != nil {
        return fmt.Errorf("failed to create temporary file when saving %q; %w", path, err)
    }

    is_closed := false
    defer func() {
        if !is_closed {
            temp.Close()
        }
    }()

    err = os.Chmod(temp.Name(), 0644)
    if err != nil {
        return fmt.Errorf("failed to set temporary file permissions when saving %q; %w", path, err);
    }

    as_str, err := json.MarshalIndent(content, "", "    ")
    if err != nil {
        return fmt.Errorf("failed to marshal JSON to save to %q; %w", path, err)
    }

    _, err = temp.Write(as_str)
    if err != nil {
        return fmt.Errorf("failed to write JSON to temporary file for %q; %w", path, err)
    }

    temp_name := temp.Name()
    is_closed = true
    err = temp.Close()
    if err != nil {
        return fmt.Errorf("failed to close temporary file when saving to %q; %w", path, err)
    }

    err = os.Rename(temp_name, path)
    if err != nil {
        return fmt.Errorf("failed to rename temporary file to %q; %w", path, err)
    }

    return nil
}

func isBadName(name string) error {
    if len(name) == 0 {
        return errors.New("name cannot be empty")
    }
    if strings.Contains(name, "/") || strings.Contains(name, "\\") {
        return errors.New("name cannot contain '/' or '\\'")
    }
    if strings.HasPrefix(name, "..") {
        return errors.New("name cannot start with '..'")
    }
    return nil
}

func isMissingOrBadName(name *string) error {
    if name == nil {
        return errors.New("missing name")
    } else {
        return isBadName(*name)
    }
}

const logDirName = "..logs"

func dumpLog(registry string, content interface{}) error {
    path := time.Now().Format(time.RFC3339) + "_" + strconv.Itoa(100000 + rand.Intn(900000))
    return dumpJson(filepath.Join(registry, logDirName, path), content)
}

func checkProjectExists(project_dir, project string) error {
    _, err := os.Stat(project_dir) 
    if errors.Is(err, os.ErrNotExist) {
        return newHttpError(http.StatusNotFound, fmt.Errorf("project %s does not exist", project))
    } else if err != nil {
        return fmt.Errorf("failed to stat %q; %w", project_dir, err)
    } else {
        return nil
    }
}

func checkAssetExists(asset_dir, asset, project string) error {
    _, err := os.Stat(asset_dir) 
    if errors.Is(err, os.ErrNotExist) {
        return newHttpError(http.StatusNotFound, fmt.Errorf("asset %s does not exist in project %s", asset, project))
    } else if err != nil {
        return fmt.Errorf("failed to stat %q; %w", asset_dir, err)
    } else {
        return nil
    }
}

func checkVersionExists(version_dir, version, asset, project string) error {
    _, err := os.Stat(version_dir)
    if errors.Is(err, os.ErrNotExist) {
        return newHttpError(http.StatusNotFound, fmt.Errorf("version %s does not exist for asset %s of project %s", version, asset, project))
    } else if err != nil {
        return fmt.Errorf("failed to stat %q; %w", version_dir, err)
    } else {
        return nil
    }
}
