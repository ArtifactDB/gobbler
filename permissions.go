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
)

type Uploader struct {
    Id *string `json:"id"`
    Asset *string `json:"asset"`
    Version *string `json:"version"`
    Until *string `json:"until"`
    Trusted *bool `json:"trusted"`
}

type Permissions struct {
    Owners []string `json:"owners"`
    Uploaders []Uploader `json:"uploaders"`
}

const PermissionsFileName = "..permissions"

func IdentifyUser(path string) (string, error) {
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

func ReadPermissions(path string) (*Permissions, error) {
    handle, err := os.ReadFile(filepath.Join(path, PermissionsFileName))
    if err != nil {
        return nil, fmt.Errorf("failed to read %q; %w", path, err)
    }

    var output Permissions
    err = json.Unmarshal(handle, &output)
    if err != nil {
        return nil, fmt.Errorf("failed to parse JSON from %q; %w", path, err)
    }

    return &output, nil
}

func IsAuthorizedToAdmin(username string, administrators []string) bool {
    if administrators != nil {
        for _, s := range administrators {
            if s == username {
                return true
            }
        }
    }
    return false
}

func IsAuthorizedToMaintain(username string, administrators []string, owners []string) bool {
    if IsAuthorizedToAdmin(username, administrators) {
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

func IsAuthorizedToUpload(username string, administrators []string, permissions *Permissions, asset, version *string) (bool, bool) {
    if IsAuthorizedToMaintain(username, administrators, permissions.Owners) {
        return true, true
    }

    if permissions.Uploaders != nil {
        for _, u := range permissions.Uploaders {
            if u.Id == nil || *(u.Id) != username {
                continue
            }

            // We accept string pointers because 'version' might not be known
            // at the time of checking permissions for the project as a whole.
            // ('asset' gets the same treatment for consistency).
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

func ValidateUploaders(uploaders []Uploader) error {
    for _, u := range uploaders {
        if u.Id == nil {
            return errors.New("all entries of 'uploaders' should have an 'id' property")
        }

        if u.Until != nil {
            _, err := time.Parse(time.RFC3339, *(u.Until))
            if err != nil {
                return errors.New("any string in 'uploaders.until' should follow the Internet Date/Time format")
            }
        }
    }

    return nil
}

func SetPermissions(path, registry string, administrators []string) error {
    incoming := struct {
        Project *string `json:"project"`
        Permissions Permissions `json:"permissions"`
    }{}
    {
        handle, err := os.ReadFile(path)
        if err != nil {
            return fmt.Errorf("failed to read %q; %w", path, err)
        }

        err = json.Unmarshal(handle, &incoming)
        if err != nil {
            return fmt.Errorf("failed to parse JSON from %q; %w", path, err)
        }

        if incoming.Project == nil {
            return fmt.Errorf("expected 'project' string in request %q", path)
        }
        err = isBadName(*(incoming.Project))
        if err != nil {
            return fmt.Errorf("invalid name for 'project' property in %q; %w", path, err)
        }
    }

    source_user, err := IdentifyUser(path)
    if err != nil {
        return fmt.Errorf("failed to find owner of %q; %w", path, err)
    }

    project := *(incoming.Project)
    project_dir := filepath.Join(registry, project)
    lock_path := filepath.Join(project_dir, LockFileName)
    handle, err := Lock(lock_path, 1000 * time.Second)
    if err != nil {
        return fmt.Errorf("failed to lock project directory %q; %w", project_dir, err)
    }
    defer Unlock(handle)

    existing, err := ReadPermissions(project_dir)
    if err != nil {
        return fmt.Errorf("failed to read permissions for %q; %w", project, err)
    }

    if !IsAuthorizedToMaintain(source_user, administrators, existing.Owners) {
        return fmt.Errorf("user %q is not authorized to modify permissions for %q", source_user, project)
    }

    if incoming.Permissions.Owners != nil {
        existing.Owners = incoming.Permissions.Owners
    }
    if incoming.Permissions.Uploaders != nil {
        err := ValidateUploaders(incoming.Permissions.Uploaders)
        if err != nil {
            return fmt.Errorf("invalid 'permissions.uploaders' in request; %w", err)
        }
        existing.Uploaders = incoming.Permissions.Uploaders
    }

    perm_path := filepath.Join(project_dir, PermissionsFileName)
    err = dumpJson(perm_path, &existing)
    if err != nil {
        return fmt.Errorf("failed to write permissions for %q; %w", project, err)
    }

    return nil
}
