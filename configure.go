package main

import (
    "fmt"
    "path/filepath"
    "encoding/json"
    "time"
    "os"
    "errors"
    "strconv"
    "strings"
    "regexp"
    "unicode"
)

func is_bad_name(name string) error {
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

func increment_series_path(prefix string, dir string) string {
    if prefix == "" {
        return filepath.Join(dir, "..series")
    } else {
        return filepath.Join(dir, "..series_" + prefix)
    }
}

func increment_series(prefix string, dir string) (string, error) {
    series_path := increment_series_path(prefix, dir)

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

func create_new_project_directory(dir string, username string, details *UploadRequest) error {
    err := os.Mkdir(dir, 0755)
    if err != nil {
        return fmt.Errorf("failed to make a new directory'; %w", err)
    }

    // Adding permissions.
    var perms Permissions;
    if details.Permissions != nil && details.Permissions.Owners != nil {
        copy(perms.Owners, details.Permissions.Owners)
    } else {
        perms.Owners = []string{ username }
    }
    if details.Permissions != nil && details.Permissions.Uploaders != nil {
        copy(perms.Uploaders, details.Permissions.Uploaders)
    } else {
        perms.Uploaders = []Uploader{}
    }

    perm_str, err := json.MarshalIndent(&perms, "", "    ")
    if err != nil {
        return fmt.Errorf("failed to convert permissions to JSON; %w", err)
    }

    err = os.WriteFile(filepath.Join(dir, "..permissions"), perm_str, 0755)
    if err != nil {
        return fmt.Errorf("failed to write permissions; %w", err)
    }

    // Dumping a mock quota and usage file for consistency with gypsum.
    // Note that the quota isn't actually enforced yet.
    err = os.WriteFile(filepath.Join(dir, "..quota"), []byte("{ \"baseline\": 1000000000, \"growth_rate\": 1000000000, \"year\": " + strconv.Itoa(time.Now().Year()) + " }"), 0755)
    if err != nil {
        return fmt.Errorf("failed to write quota for '" + dir + "'; %w", err)
    }

    err = os.WriteFile(filepath.Join(dir, "..usage"), []byte("{ \"total\": 0 }"), 0755)
    if err != nil {
        return fmt.Errorf("failed to write usage for '" + dir + "'; %w", err)
    }

    return nil
}

func process_project(registry string, username string, details *UploadRequest) (string, bool, error) {
    lock_path := filepath.Join(registry, LockFileName)
    handle, err := Lock(lock_path, 1000 * time.Second)
    if err != nil {
        return "", false, fmt.Errorf("failed to acquire the global registry lock; %w", err)
    }
    defer Unlock(handle)

    // Creating a new project from a series.
    if details.Project == nil {
        if details.Prefix == nil {
            return "", false, errors.New("expected a 'prefix' property in the request details")
        }
        prefix := *(details.Prefix)
        re, _ := regexp.Compile("^[A-Z]+$")
        if !re.MatchString(prefix) {
            return "", false, errors.New("prefix must contain only uppercase letters (got '" + prefix + "')")
        }

        candidate_name, err := increment_series(prefix, registry)
        if err != nil {
            return "", false, err
        }

        err = create_new_project_directory(filepath.Join(registry, candidate_name), username, details)
        if err != nil {
            return "", false, fmt.Errorf("failed to populate internals for '" + candidate_name + "'; %w", err)
        }

        return candidate_name, true, nil
    }

    // Creating a new project from a pre-supplied name.
    project := *(details.Project)
    err = is_bad_name(project)
    if err != nil {
        return "", false, fmt.Errorf("invalid project name; %w", err)
    }

    project_dir := filepath.Join(registry, project)
    info, err := os.Stat(project_dir)
    if errors.Is(err, os.ErrNotExist) {
        if unicode.IsUpper(rune(project[0])) {
            return "", false, errors.New("new user-supplied project names should not start with an uppercase letter")
        }

        err = create_new_project_directory(filepath.Join(registry, project), username, details)
        if err != nil {
            return "", false, fmt.Errorf("failed to populate internals for '" + project + "'; %w", err)
        }

        return project, true, nil
    }

    // Updating an existing directory.
    if err != nil || !info.IsDir() {
        return "", false, fmt.Errorf("failed to inspect an existing project directory '" + project + "'; %w", err)
    }

    perm_path := filepath.Join(project_dir, "..permissions")
    perm_handle, err := os.ReadFile(perm_path)
    if err != nil {
        return "", false, fmt.Errorf("failed to read permissions for '" + project + "'; %w", err)
    }

    var perms Permissions
    err = json.Unmarshal(perm_handle, &perms)
    if err != nil {
        return "", false, fmt.Errorf("failed to parse JSON from '" + perm_path + "'; %w", err)
    }

    // Only checking owners right now; support for uploaders is not yet implemented.
    okay := false
    for _, s := range(perms.Owners) {
        if s == username {
            okay = true
        }
    }
    if !okay {
        return "", false, fmt.Errorf("user '" + username + "' is not listed as an owner for '" + project + "'")
    }

    return project, false, nil
}

func process_asset(project_dir string, details *UploadRequest) (string, error) {
    // No need to lock here, as multiple processes can be requesting the same
    // asset at once... it's the versions we need to be worrying about.
    if details.Asset == nil {
        return "", errors.New("expected an 'asset' property in the request details")
    }

    err := is_bad_name(*details.Asset)
    if err != nil {
        return "", fmt.Errorf("invalid asset name; %w", err)
    }

    asset := *(details.Asset)
    asset_dir := filepath.Join(project_dir, asset)
    if _, err := os.Stat(asset_dir); errors.Is(err, os.ErrNotExist) {
        err = os.Mkdir(asset_dir, 0755)
        if err != nil {
            return "", fmt.Errorf("failed to create a new asset directory inside '" + project_dir + "'; %w", err)
        }
    }

    return asset, nil
}

func process_version(asset_dir string, is_new_project bool, details *UploadRequest) (string, error) {
    lock_path := filepath.Join(asset_dir, LockFileName)
    handle, err := Lock(lock_path, 1000 * time.Second)
    if err != nil {
        return "", fmt.Errorf("failed to acquire the lock on the asset directory %q; %w", asset_dir, err)
    }
    defer Unlock(handle)

    series_path := increment_series_path("", asset_dir)

    // Creating a new version from a series.
    if details.Version == nil {
        if _, err := os.Stat(series_path); errors.Is(err, os.ErrNotExist) {
            if !is_new_project { // check it's not a newly created project, in which case it wouldn't have a series yet.
                return "", errors.New("must provide 'version' in '" + asset_dir + "' initialized without a version series")
            }
        }

        candidate_name, err := increment_series("", asset_dir)
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
    err = is_bad_name(*details.Version)
    if err != nil {
        return "", fmt.Errorf("invalid version name; %w", err)
    }

    if _, err := os.Stat(series_path); err == nil {
        return "", errors.New("cannot use user-supplied 'version' in '" + asset_dir + "' initialized with a version series")
    }
    version := *(details.Version)

    candidate_path := filepath.Join(asset_dir, version)
    if _, err := os.Stat(candidate_path); err == nil {
        return "", errors.New("version '" + version + "' already exists in '" + asset_dir + "'")
    }

    err = os.Mkdir(candidate_path, 0755)
    if err != nil {
        return "", fmt.Errorf("failed to create a new version directory at '" + candidate_path + "'; %w", err)
    }

    return version, nil
}

type Configuration struct {
    Project string
    Asset string
    Version string
    User string
}

func Configure(request *UploadRequest, registry string) (*Configuration, error) {
    details_path := request.Self
    if request.Source == nil {
        return nil, fmt.Errorf("expected a 'source' property in the upload request at %q", details_path)
    }
    source := *(request.Source)

    username, err := IdentifyUser(source)
    if err != nil {
        return nil, fmt.Errorf("failed to identify the user for '" + source + "'; %w", err)
    }

    project, is_new, err := process_project(registry, username, request)
    if err != nil {
        return nil, fmt.Errorf("failed to process the project for '" + source + "'; %w", err)
    }

    project_dir := filepath.Join(registry, project)
    asset, err := process_asset(project_dir, request)
    if err != nil {
        return nil, fmt.Errorf("failed to process the asset for '" + source + "'; %w", err)
    }

    asset_dir := filepath.Join(project_dir, asset)
    version, err := process_version(asset_dir, is_new, request)
    if err != nil {
        return nil, fmt.Errorf("failed to process the version for '" + source + "'; %w", err)
    }

    return &Configuration{
        Project: project,
        Asset: asset,
        Version: version,
        User: username,
    }, nil
}
