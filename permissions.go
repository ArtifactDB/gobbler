package main

import (
    "os"
    "fmt"
    "errors"
    "syscall"
    "os/user"
    "strconv"
    "path/filepath"
    "encoding/json"
    "time"
    "net/http"
)

type uploaderEntry struct {
    Id string `json:"id"`
    Asset *string `json:"asset,omitempty"`
    Version *string `json:"version,omitempty"`
    Until *string `json:"until,omitempty"`
    Trusted *bool `json:"trusted,omitempty"`
}

type permissionsMetadata struct {
    Owners []string `json:"owners"`
    Uploaders []uploaderEntry `json:"uploaders"`
    GlobalWrite *bool `json:"global_write,omitempty"`
}

const permissionsFileName = "..permissions"

func identifyUser(path string) (string, error) {
    sinfo, err := os.Stat(path)
    if err != nil {
        return "", fmt.Errorf("failed to inspect '" + path + "'; %w", err)
    }

    stat, ok := sinfo.Sys().(*syscall.Stat_t)
    if !ok {
        return "", errors.New("failed to determine author of '" + path + "'")
    }

    uinfo, err := user.LookupId(strconv.Itoa(int(stat.Uid)))
    if !ok {
        return "", fmt.Errorf("failed to find user name for author of '" + path + "'; %w", err)
    }
    return uinfo.Username, nil
}

func readPermissions(path string) (*permissionsMetadata, error) {
    handle, err := os.ReadFile(filepath.Join(path, permissionsFileName))
    if err != nil {
        return nil, fmt.Errorf("failed to read %q; %w", path, err)
    }

    var output permissionsMetadata
    err = json.Unmarshal(handle, &output)
    if err != nil {
        return nil, fmt.Errorf("failed to parse JSON from %q; %w", path, err)
    }

    return &output, nil
}

func isAuthorizedToAdmin(username string, administrators []string) bool {
    if administrators != nil {
        for _, s := range administrators {
            if s == username {
                return true
            }
        }
    }
    return false
}

func isAuthorizedToMaintain(username string, administrators []string, owners []string) bool {
    if isAuthorizedToAdmin(username, administrators) {
        return true
    }
    if owners != nil {
        for _, s := range owners {
            if s == username {
                return true
            }
        }
    }
    return false
}

func isAuthorizedToUpload(username string, administrators []string, permissions *permissionsMetadata, asset, version *string) (bool, bool) {
    if isAuthorizedToMaintain(username, administrators, permissions.Owners) {
        return true, true
    }

    if permissions.Uploaders != nil {
        for _, u := range permissions.Uploaders {
            if u.Id != username {
                continue
            }

            // We accept string pointers because 'version' might not be known
            // at the time of checking permissions for the project as a whole.
            if u.Asset != nil && (asset == nil || *(u.Asset) != *asset) {
                continue
            }
            if u.Version != nil && (version == nil || *(u.Version) != *version) {
                continue
            }

            if u.Until != nil {
                parsed, err := time.Parse(time.RFC3339, *(u.Until))
                if err != nil {
                    continue
                }
                if parsed.Before(time.Now()) {
                    continue
                }
            }

            return true, (u.Trusted != nil && *(u.Trusted))
        }
    }

    return false, false
}

func prepareGlobalWriteNewAsset(username string, permissions *permissionsMetadata, asset string, project_dir string) (bool, error) {
    if permissions.GlobalWrite == nil || !*(permissions.GlobalWrite) {
        return false, nil
    }

    asset_dir := filepath.Join(project_dir, asset)
    _, err := os.Stat(asset_dir)

    if err == nil || !errors.Is(err, os.ErrNotExist) {
        return false, nil
    }

    // Updating the permissions in memory and on disk.
    is_trusted := true
    permissions.Uploaders = append(permissions.Uploaders, uploaderEntry{ Id: username, Asset: &asset, Trusted: &is_trusted })

    perm_path := filepath.Join(project_dir, permissionsFileName)
    err = dumpJson(perm_path, permissions)
    if err != nil {
        return false, err
    }

    return true, nil
}

func sanitizeUploaders(uploaders []unsafeUploaderEntry) ([]uploaderEntry, error) {
    output := make([]uploaderEntry, len(uploaders))

    for i, u := range uploaders {
        if u.Id == nil {
            return nil, errors.New("all entries of 'uploaders' should have an 'id' property")
        }

        if u.Until != nil {
            _, err := time.Parse(time.RFC3339, *(u.Until))
            if err != nil {
                return nil, errors.New("any string in 'uploaders.until' should follow the Internet Date/Time format")
            }
        }

        output[i].Id = *(u.Id)
        output[i].Asset = u.Asset
        output[i].Version = u.Version
        output[i].Until = u.Until
        output[i]. Trusted = u.Trusted
    }

    return output, nil
}

type unsafeUploaderEntry struct {
    Id *string `json:"id"`
    Asset *string `json:"asset"`
    Version *string `json:"version"`
    Until *string `json:"until"`
    Trusted *bool `json:"trusted"`
}

type unsafePermissionsMetadata struct {
    Owners []string `json:"owners"`
    Uploaders []unsafeUploaderEntry `json:"uploaders"`
    GlobalWrite *bool `json:"global_write"`
}

func setPermissionsHandler(reqpath string, globals *globalConfiguration) error {
    incoming := struct {
        Project *string `json:"project"`
        Permissions *unsafePermissionsMetadata `json:"permissions"`
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

        if incoming.Permissions == nil {
            return newHttpError(http.StatusBadRequest, fmt.Errorf("expected a 'permissions' object in %q", reqpath))
        }
    }

    source_user, err := identifyUser(reqpath)
    if err != nil {
        return fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }

    project := *(incoming.Project)
    project_dir := filepath.Join(globals.Registry, project)
    err = globals.Locks.LockDirectory(project_dir, 10 * time.Second)
    if err != nil {
        return fmt.Errorf("failed to lock project directory %q; %w", project_dir, err)
    }
    defer globals.Locks.Unlock(project_dir)

    existing, err := readPermissions(project_dir)
    if err != nil {
        return fmt.Errorf("failed to read permissions for %q; %w", project, err)
    }

    if !isAuthorizedToMaintain(source_user, globals.Administrators, existing.Owners) {
        return newHttpError(http.StatusForbidden, fmt.Errorf("user %q is not authorized to modify permissions for %q", source_user, project))
    }

    if incoming.Permissions.Owners != nil {
        existing.Owners = incoming.Permissions.Owners
    }
    if incoming.Permissions.Uploaders != nil {
        san, err := sanitizeUploaders(incoming.Permissions.Uploaders)
        if err != nil {
            return newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'permissions.uploaders' in request; %w", err))
        }
        existing.Uploaders = san
    }
    if incoming.Permissions.GlobalWrite != nil {
        existing.GlobalWrite = incoming.Permissions.GlobalWrite
    }

    perm_path := filepath.Join(project_dir, permissionsFileName)
    err = dumpJson(perm_path, existing)
    if err != nil {
        return fmt.Errorf("failed to write permissions for %q; %w", project, err)
    }

    return nil
}
