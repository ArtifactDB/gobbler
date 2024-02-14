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

func incrementSeriesPath(prefix string, dir string) string {
    if prefix == "" {
        return filepath.Join(dir, "..series")
    } else {
        return filepath.Join(dir, "..series_" + prefix)
    }
}

func incrementSeries(prefix string, dir string) (string, error) {
    series_path := incrementSeriesPath(prefix, dir)

    num := 0
    if _, err := os.Stat(series_path); err == nil {
        content, err := os.ReadFile(series_path)
        if err != nil {
            return "", fmt.Errorf("failed to read '" + series_path + "'; %w", err)
        }
        num, err = strconv.Atoi(string(content))
        if err != nil {
            return "", fmt.Errorf("failed to determine latest number from '" + series_path + "; %w", err)
        }
    }

    num += 1
    as_str := strconv.Itoa(num)
    candidate_name := prefix + as_str

    // Checking that it doesn't already exist.
    if _, err := os.Stat(filepath.Join(dir, candidate_name)); err == nil {
        dhandle, err := os.Open(dir)
        if err != nil {
            return "", fmt.Errorf("failed to obtain a handle for the output directory; %w", err)
        }

        all_names, err := dhandle.Readdirnames(-1)
        if err != nil {
            return "", fmt.Errorf("failed to read subdirectories of the output directory; %w", err)
        }
        if prefix != "" {
            for _, subdir := range all_names {
                if strings.HasPrefix(subdir, prefix) {
                    curnum, err := strconv.Atoi(strings.TrimPrefix(subdir, prefix))
                    if err != nil && curnum > num {
                        num = curnum
                    }
                }
            }
        } else {
            for _, subdir := range all_names {
                curnum, err := strconv.Atoi(subdir)
                if err != nil && curnum > num {
                    num = curnum
                }
            }
        }

        num += 1
        as_str = strconv.Itoa(num)
        candidate_name = prefix + as_str
    }

    // Updating the series.
    err := os.WriteFile(series_path, []byte(as_str), 0644)
    if err != nil {
        return "", fmt.Errorf("failed to update the series counter for '" + prefix + "'; %w", err)
    }

    return candidate_name, nil
}

func configureProject(registry string, username string, project, prefix *string, locks *pathLocks) (string, bool, error) {
    // Creating a new project from a series.
    if project == nil {
        if prefix == nil {
            return "", false, errors.New("expected a 'prefix' property in the request details")
        }
        prefix_str := *(prefix)
        re, _ := regexp.Compile("^[A-Z]+$")
        if !re.MatchString(prefix_str) {
            return "", false, fmt.Errorf("prefix must contain only uppercase letters (got %q)", prefix_str)
        }

        // Obtaining a global lock to avoid simultaneous increments.
        prefix_path := filepath.Join(registry, prefix_str)
        err := locks.LockPath(prefix_path, 1000 * time.Second)
        if err != nil {
            return "", false, fmt.Errorf("failed to acquire the global registry lock; %w", err)
        }
        defer locks.UnlockPath(prefix_path)

        candidate_name, err := incrementSeries(prefix_str, registry)
        if err != nil {
            return "", false, err
        }

        err = os.Mkdir(filepath.Join(registry, candidate_name), 0755)
        if err != nil {
            return "", false, fmt.Errorf("failed to make a new directory'; %w", err)
        }

        return candidate_name, true, nil
    }

    project_str := *project
    err := isBadName(project_str)
    if err != nil {
        return "", false, fmt.Errorf("invalid project name; %w", err)
    }

    // Creating a new project from a pre-supplied name.
    project_dir := filepath.Join(registry, project_str)
    info, err := os.Stat(project_dir)
    if errors.Is(err, os.ErrNotExist) {
        if unicode.IsUpper(rune(project_str[0])) {
            return "", false, errors.New("new user-supplied project names should not start with an uppercase letter")
        }

        // No need to lock here, MkdirAll just no-ops if the directory already exists.
        err := os.MkdirAll(filepath.Join(registry, project_str), 0755)
        if err != nil {
            return "", false, fmt.Errorf("failed to make a new directory'; %w", err)
        }

        return project_str, true, nil
    }

    // Updating an existing directory.
    if err != nil || !info.IsDir() {
        return "", false, fmt.Errorf("failed to inspect an existing project directory %q; %w", project_str, err)
    }

    return project_str, false, nil
}

func populateNewProjectDirectory(dir string, username string, permissions *unsafePermissionsMetadata) error {
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

    return nil
}

func configureAsset(project_dir string, asset *string) (string, bool, error) {
    if asset == nil {
        return "", false, errors.New("expected an 'asset' property in the request details")
    }

    asset_str := *asset
    err := isBadName(asset_str)
    if err != nil {
        return "", false, fmt.Errorf("invalid asset name %q; %w", asset_str, err)
    }

    asset_dir := filepath.Join(project_dir, asset_str)
    is_new := false
    if _, err := os.Stat(asset_dir); errors.Is(err, os.ErrNotExist) {
        err = os.Mkdir(asset_dir, 0755)
        if err != nil {
            return "", false, fmt.Errorf("failed to create a new asset directory inside %q; %w", asset_dir, err)
        }
        is_new = true
    }

    return asset_str, is_new, nil
}

func configureVersion(asset_dir string, is_new_project bool, version *string) (string, error) {
    series_path := incrementSeriesPath("", asset_dir)

    // Creating a new version from a series.
    if version == nil {
        if _, err := os.Stat(series_path); errors.Is(err, os.ErrNotExist) {
            if !is_new_project { // check it's not a newly created project, in which case it wouldn't have a series yet.
                return "", errors.New("must provide 'version' in '" + asset_dir + "' initialized without a version series")
            }
        }

        candidate_name, err := incrementSeries("", asset_dir)
        if err != nil {
            return "", err
        }

        candidate_path := filepath.Join(asset_dir, candidate_name)
        err = os.Mkdir(candidate_path, 0755)
        if err != nil {
            return "", fmt.Errorf("failed to create a new version directory at '" + candidate_path + "'; %w", err)
        }

        return candidate_name, nil
    }

    // Otherwise using the user-supplied version name.
    if _, err := os.Stat(series_path); err == nil {
        return "", errors.New("cannot use user-supplied 'version' in '" + asset_dir + "' initialized with a version series")
    }

    version_str := *version
    err := isBadName(version_str)
    if err != nil {
        return "", fmt.Errorf("invalid version name %q; %w", version_str, err)
    }

    candidate_path := filepath.Join(asset_dir, version_str)
    if _, err := os.Stat(candidate_path); err == nil {
        return "", fmt.Errorf("version %q already exists in %q", version_str, asset_dir)
    }

    err = os.Mkdir(candidate_path, 0755)
    if err != nil {
        return "", fmt.Errorf("failed to create a new version directory at %q; %w", candidate_path, err)
    }

    return version_str, nil
}

type uploadConfiguration struct {
    Project string
    Version string
}

func uploadHandler(reqpath string, globals *globalConfiguration) (*uploadConfiguration, error) {
    request := struct {
        Source *string `json:"source"`
        Prefix *string `json:"prefix"`
        Project *string `json:"project"`
        Asset *string `json:"asset"`
        Version *string `json:"version"`
        Permissions *unsafePermissionsMetadata `json:"permissions"`
        OnProbation *bool `json:"on_probation"`
    }{}

    upload_start := time.Now()

    req_user, err := identifyUser(reqpath)
    if err != nil {
        return nil, fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }

    var source string

    // Reading in the request.
    {
        handle, err := os.ReadFile(reqpath)
        if err != nil {
            return nil, fmt.Errorf("failed to read %q; %w", reqpath, err)
        }
        err = json.Unmarshal(handle, &request)
        if err != nil {
            return nil, fmt.Errorf("failed to parse JSON from %q; %w", reqpath, err)
        }

        if request.Source == nil {
            return nil, fmt.Errorf("expected a 'source' property in %q; %w", reqpath, err)
        }
        source = *(request.Source)

        source_user, err := identifyUser(source)
        if err != nil {
            return nil, fmt.Errorf("failed to find owner of %q; %w", source, err)
        }
        if source_user != req_user {
            return nil, fmt.Errorf("requesting user must be the same as the owner of the 'source' directory (%s vs %s)", source_user, req_user)
        }
    }

    on_probation := request.OnProbation != nil && *(request.OnProbation)

    // Configuring the project; we apply a lock to the project to avoid concurrent changes.
    project, is_new_project, err := configureProject(globals.Registry, req_user, request.Project, request.Prefix, &(globals.Locks))
    if err != nil {
        return nil, fmt.Errorf("failed to process the project for '" + source + "'; %w", err)
    }

    project_dir := filepath.Join(globals.Registry, project)
    err = globals.Locks.LockPath(project_dir, 1000 * time.Second)
    if err != nil {
        return nil, fmt.Errorf("failed to acquire the lock on %q; %w", project_dir, err)
    }
    defer globals.Locks.UnlockPath(project_dir)

    if !is_new_project {
        perms, err := readPermissions(project_dir)
        if err != nil {
            return nil, fmt.Errorf("failed to read permissions for %q; %w", project, err)
        }
        ok, trusted := isAuthorizedToUpload(req_user, globals.Administrators, perms, request.Asset, request.Version)
        if !ok {
            return nil, fmt.Errorf("user '" + req_user + "' is not authorized to upload to '" + project + "'")
        }
        if !trusted {
            on_probation = true
        }
    } else {
        err := populateNewProjectDirectory(project_dir, req_user, request.Permissions)
        if err != nil {
            return nil, fmt.Errorf("failed to populate project metadata for request %q; %w", reqpath, err)
        }
    }

    // Configuring the asset and version.
    asset, is_new_asset, err := configureAsset(project_dir, request.Asset)
    if err != nil {
        return nil, fmt.Errorf("failed to configure the asset for request %q; %w", reqpath, err)
    }
    asset_dir := filepath.Join(project_dir, asset)

    version, err := configureVersion(asset_dir, is_new_asset, request.Version)
    if err != nil {
        return nil, fmt.Errorf("failed to configure the version for request %q; %w", reqpath, err)
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
        return nil, fmt.Errorf("failed to transfer files from %q; %w", source, err)
    }

    // Writing out the various pieces of metadata. This should be, in theory,
    // the 'no-throw' section as no user-supplied values are involved here.
    version_dir := filepath.Join(asset_dir, version)
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
            return nil, fmt.Errorf("failed to save summary for %q; %w", asset_dir, err)
        }
    }

    {
        extra, err := computeUsage(version_dir, true)
        if err != nil {
            return nil, fmt.Errorf("failed to compute usage for the new version at %q; %w", version_dir, err)
        }

        usage, err := readUsage(project_dir)
        if err != nil {
            return nil, fmt.Errorf("failed to read existing usage for project %q; %w", project, err)
        }
        usage.Total += extra

        // Try to do this write later to reduce the chance of an error
        // triggering an abort after the usage total has been updated.
        usage_path := filepath.Join(project_dir, usageFileName)
        err = dumpJson(usage_path, &usage)
        if err != nil {
            return nil, fmt.Errorf("failed to save usage for %q; %w", project_dir, err)
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
            return nil, fmt.Errorf("failed to save latest version for %q; %w", asset_dir, err)
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
            return nil, fmt.Errorf("failed to save log file; %w", err)
        }
    }

    has_failed = false
    return &uploadConfiguration{ Project: project, Version: version }, nil
}
