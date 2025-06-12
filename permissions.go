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
    "context"
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
    contents, err := os.ReadFile(filepath.Join(path, permissionsFileName))
    if err != nil {
        return nil, fmt.Errorf("failed to read %q; %w", path, err)
    }

    var output permissionsMetadata
    err = json.Unmarshal(contents, &output)
    if err != nil {
        return nil, fmt.Errorf("failed to parse JSON from %q; %w", path, err)
    }

    return &output, nil
}

func addAssetPermissionsForUpload(existing *permissionsMetadata, asset_dir, asset string) (*permissionsMetadata, error) {
    path := filepath.Join(asset_dir, permissionsFileName)
    contents, err := os.ReadFile(path)

    if err != nil {
        if errors.Is(err, os.ErrNotExist) {
            return existing, nil
        } else {
            return nil, fmt.Errorf("failed to read %q; %w", path, err)
        }
    }

    // If we need to modify the permissions, we return a new object to avoid mutating the input object.
    var loaded permissionsMetadata
    err = json.Unmarshal(contents, &loaded)
    if err != nil {
        return nil, fmt.Errorf("failed to parse JSON from %q; %w", path, err)
    }

    loaded.Owners = append(existing.Owners, (loaded.Owners)...)

    new_uploaders := existing.Uploaders
    for _, up := range loaded.Uploaders {
        up.Asset = &asset
        new_uploaders = append(new_uploaders, up)
    }
    loaded.Uploaders = new_uploaders

    return &loaded, nil
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
        output[i].Trusted = u.Trusted
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

func setPermissionsHandler(reqpath string, globals *globalConfiguration, ctx context.Context) error {
    incoming := struct {
        Project *string `json:"project"`
        Asset *string `json:"asset"`
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
            return newHttpError(http.StatusBadRequest, fmt.Errorf("missing or invalid 'project' property in %q; %w", reqpath, err))
        }

        if incoming.Asset != nil {
            err := isBadName(*(incoming.Asset))
            if err != nil {
                return newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'asset' property in %q; %w", reqpath, err))
            }
        }

        if incoming.Permissions == nil {
            return newHttpError(http.StatusBadRequest, fmt.Errorf("expected a 'permissions' object in %q", reqpath))
        }
    }

    source_user, err := identifyUser(reqpath)
    if err != nil {
        return fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }

    rlock, err := lockDirectoryShared(globals, globals.Registry, ctx)
    if err != nil {
        return fmt.Errorf("failed to lock the registry %q; %w", globals.Registry, err)
    }
    defer rlock.Unlock(globals)

    project := *(incoming.Project)
    project_dir := filepath.Join(globals.Registry, project)
    if err := checkProjectExists(project_dir, project); err != nil {
        return err
    }

    // If Asset is provided, we could consider holding a shared lock to improve parallelism.
    // However, this gets complicated if the asset directory does not exist, in which case we need to reacquire an exclusive lock to create the directory.
    // It's just simpler to hold an exclusive lock to start with, the handler should finish up pretty quickly so any contention is limited.
    plock, err := lockDirectoryExclusive(globals, project_dir, ctx)
    if err != nil {
        return fmt.Errorf("failed to lock the project directory %q; %w", project_dir, err)
    }
    defer plock.Unlock(globals)

    project_perms, err := readPermissions(project_dir)
    if err != nil {
        return fmt.Errorf("failed to read permissions for %q; %w", project, err)
    }

    if incoming.Asset == nil {
        if !isAuthorizedToMaintain(source_user, globals.Administrators, project_perms.Owners) {
            return newHttpError(http.StatusForbidden, fmt.Errorf("user %q is not authorized to modify permissions for %q", source_user, project))
        }

        if incoming.Permissions.Owners != nil {
            project_perms.Owners = incoming.Permissions.Owners
        }
        if incoming.Permissions.Uploaders != nil {
            san, err := sanitizeUploaders(incoming.Permissions.Uploaders)
            if err != nil {
                return newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'permissions.uploaders' in request; %w", err))
            }
            project_perms.Uploaders = san
        }
        if incoming.Permissions.GlobalWrite != nil {
            project_perms.GlobalWrite = incoming.Permissions.GlobalWrite
        }

        perm_path := filepath.Join(project_dir, permissionsFileName)
        err = dumpJson(perm_path, project_perms)
        if err != nil {
            return fmt.Errorf("failed to write permissions for %q; %w", project, err)
        }

    } else {
        asset := *(incoming.Asset)
        asset_dir := filepath.Join(project_dir, asset)
        var asset_perms *permissionsMetadata

        asset_perm_path := filepath.Join(asset_dir, permissionsFileName)
        _, err := os.Stat(asset_perm_path)
        if err == nil {
            existing, err := readPermissions(asset_dir)
            if err != nil {
                return fmt.Errorf("failed to read permissions for asset %q in %q; %w", asset, project, err)
            }
            asset_perms = existing
        } else if errors.Is(err, os.ErrNotExist) {
            asset_perms = &permissionsMetadata{ Owners: []string{}, Uploaders: []uploaderEntry{} }
        } else {
            return fmt.Errorf("failed to stat asset permissions in %q; %w", asset_dir, err)
        }

        combined_owners := append(project_perms.Owners, (asset_perms.Owners)...)
        if !isAuthorizedToMaintain(source_user, globals.Administrators, combined_owners) {
            return newHttpError(http.StatusForbidden, fmt.Errorf("user %q is not authorized to modify permissions for asset %q in %q", source_user, asset, project))
        }

        if incoming.Permissions.Owners != nil {
            asset_perms.Owners = incoming.Permissions.Owners
        }
        if incoming.Permissions.Uploaders != nil {
            san, err := sanitizeUploaders(incoming.Permissions.Uploaders)
            if err != nil {
                return newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'permissions.uploaders' in request; %w", err))
            }
            for i, _ := range san {
                san[i].Asset = nil
            }
            asset_perms.Uploaders = san
        }

        _, err = os.Stat(asset_dir)
        if err != nil {
            if errors.Is(err, os.ErrNotExist) {
                err := os.Mkdir(asset_dir, 0755)
                if err != nil {
                    return fmt.Errorf("failed to create new asset directory %q; %w", asset_dir, err)
                }
            } else {
                return fmt.Errorf("failed to stat asset directory %q; %w", asset_dir, err)
            }
        }

        err = dumpJson(asset_perm_path, asset_perms)
        if err != nil {
            return fmt.Errorf("failed to write asset-level permissions for %q; %w", asset_dir, err)
        }
    }

    return nil
}
