package main

import (
    "fmt"
    "time"
    "path/filepath"
    "os"
    "encoding/json"
    "errors"
    "net/http"
    "sync"
    "math/rand"
    "strings"
)

type uploadRequest struct {
    Source *string `json:"source"`
    Project *string `json:"project"`
    Asset *string `json:"asset"`
    Version *string `json:"version"`
    OnProbation *bool `json:"on_probation"`
}

type uploadRequestPlus struct {
    Base *uploadRequest
    User string
    StartTime time.Time
}

type uploadRequestRegistry struct {
    NumPools int
    Locks []sync.Mutex
    Requests []map[string]uploadRequestPlus

    TokenLength int
    TokenMaxAttempts int
    TokenExpiry time.Duration
}

func newUploadRequestRegistry(num_pools int) *uploadRequestRegistry {
    return &uploadRequestRegistry{ 
        NumPools: num_pools,
        Locks: make([]sync.Mutex, num_pools),
        Requests: make([]map[string]uploadRequestPlus, num_pools),

        TokenLength: 25,
        TokenMaxAttempts: 5,
        TokenExpiry: time.Minute,
    }
}

const uploadTokenAlphanumerics = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func (u *uploadRequestRegistry) Add(req *uploadRequest, user string, start_time time.Time) (string, error) {
    for attempt := 0; attempt < u.TokenMaxAttempts; attempt++ {
        raw_token := make([]byte, u.TokenLength)
        denom := int32(len(uploadTokenAlphanumerics))
        for i := 0; i < u.TokenLength; i++ {
            raw_token[i] = uploadTokenAlphanumerics[rand.Int31n(denom)]
        }

        token := string(raw_token)
        pool := chooseLockPool(token, u.NumPools)

        success := func() bool { // wrap in a function so that defer() runs correctly.
            u.Locks[pool].Lock()
            defer u.Locks[pool].Unlock()

            if u.Requests[pool] == nil {
                u.Requests[pool] = make(map[string]uploadRequestPlus)
            }

            _, ok := u.Requests[pool][token]
            if !ok {
                u.Requests[pool][token] = uploadRequestPlus{ Base: req, User: user, StartTime: start_time }

                go func() { // launching a mop-up job to remove the expired request if it isn't already gone.
                    time.Sleep(u.TokenExpiry)
                    u.Locks[pool].Lock()
                    defer u.Locks[pool].Unlock()

                    info, ok := u.Requests[pool][token]
                    if !ok {
                        return
                    }

                    now := time.Now()
                    if now.Sub(info.StartTime) >= u.TokenExpiry {
                        delete(u.Requests[pool], token)
                    }
                }()

                return true 
            } else {
                return false
            }
        }()

        if success {
            return token, nil
        }
    }

    return "", errors.New("failed to obtain a unique token for upload")
}

func (u *uploadRequestRegistry) Pop(token string) (*uploadRequest, string, time.Time) {
    pool := chooseLockPool(token, u.NumPools)
    u.Locks[pool].Lock()
    defer u.Locks[pool].Unlock()

    info, ok := u.Requests[pool][token]
    if ok {
        delete(u.Requests[pool], token)
        return info.Base, info.User, info.StartTime
    } else {
        return nil, "", time.Time{}
    }
}

func uploadPreflightHandler(reqpath string, upreg *uploadRequestRegistry) (string, error) {
    upload_start := time.Now()

    req_user, err := identifyUser(reqpath)
    if err != nil {
        return "", fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }
    handle, err := os.ReadFile(reqpath)
    if err != nil {
        return "", fmt.Errorf("failed to read %q; %w", reqpath, err)
    }

    request := uploadRequest{}
    err = json.Unmarshal(handle, &request)
    if err != nil {
        return "", newHttpError(http.StatusBadRequest, fmt.Errorf("failed to parse JSON from %q; %w", reqpath, err))
    }

    // Checking the source.
    if request.Source == nil {
        return "", newHttpError(http.StatusBadRequest, fmt.Errorf("expected a 'source' property in %q; %w", reqpath, err))
    }
    source := *(request.Source)
    if !filepath.IsAbs(source) {
        return "", newHttpError(http.StatusBadRequest, errors.New("'source' should be an absolute path"))
    }

    // Now checking through all of the project/asset/version names.
    if request.Project == nil {
        return "", newHttpError(http.StatusBadRequest, fmt.Errorf("expected a 'project' property in %q", reqpath))
    }
    project := *(request.Project)
    if isBadName(project) != nil {
        return "", newHttpError(http.StatusBadRequest, fmt.Errorf("invalid project name %q; %w", project, err))
    }

    if request.Asset == nil {
        return "", newHttpError(http.StatusBadRequest, fmt.Errorf("expected a 'asset' property in %q", reqpath))
    }
    asset := *(request.Asset)
    if isBadName(asset) != nil {
        return "", newHttpError(http.StatusBadRequest, fmt.Errorf("invalid asset name %q; %w", asset, err))
    }

    if request.Version == nil {
        return "", newHttpError(http.StatusBadRequest, fmt.Errorf("expected a 'version' property in %q", reqpath))
    }
    version := *(request.Version)
    if isBadName(version) != nil {
        return "", newHttpError(http.StatusBadRequest, fmt.Errorf("invalid version name %q; %w", version, err))
    }

    token, err := upreg.Add(&request, req_user, upload_start)
    return token, err
}

func uploadHandler(token string, upreg *uploadRequestRegistry, path string, globals *globalConfiguration) error {
    request, req_user, upload_start := upreg.Pop(token)
    if request == nil {
        return newHttpError(http.StatusBadRequest, errors.New("no upload request corresponding to this token"))
    }

    // Verifying that the user had write access to the source directory.
    if path != filepath.Base(path) {
        return newHttpError(http.StatusBadRequest, errors.New("'path' should refer to a file in the 'source' directory"))
    }
    if !strings.HasPrefix(path, ".") {
        return newHttpError(http.StatusBadRequest, errors.New("'path' should refer to a dotfile")) // to avoid inclusion during transfer.
    }
    source := *(request.Source)
    contents, err := os.ReadFile(filepath.Join(source, path))
    if err != nil {
        return newHttpError(http.StatusBadRequest, errors.New("could not read the specified 'path' inside the 'source' directory"))
    }
    if string(contents) != token {
        return newHttpError(http.StatusBadRequest, errors.New("contents of the 'path' are not equal to the 'token'"))
    }

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

    ok, trusted := isAuthorizedToUpload(req_user, globals.Administrators, perms, request.Asset, request.Version)
    if !ok {
        return newHttpError(http.StatusForbidden, fmt.Errorf("user '" + req_user + "' is not authorized to upload to '" + project + "'"))
    }

    on_probation := request.OnProbation != nil && *(request.OnProbation)
    if !trusted {
        on_probation = true
    }

    // Configuring the asset and version.
    asset := *(request.Asset)
    asset_dir := filepath.Join(project_dir, asset)
    if _, err := os.Stat(asset_dir); errors.Is(err, os.ErrNotExist) {
        err = os.Mkdir(asset_dir, 0755)
        if err != nil {
            return fmt.Errorf("failed to create a new asset directory %q; %w", asset_dir, err)
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
