package main

import (
    "fmt"
    "time"
    "path/filepath"
    "os"
    "encoding/json"
    "strconv"
    "errors"
    "net/http"
    "context"
)

func createProjectHandler(reqpath string, globals *globalConfiguration, ctx context.Context) error {
    req_user, err := identifyUser(reqpath)
    if err != nil {
        return fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }
    if !isAuthorizedToAdmin(req_user, globals.Administrators) {
        return newHttpError(http.StatusForbidden, fmt.Errorf("user %q is not authorized to create a project", req_user))
    }

    request := struct {
        Project *string `json:"project"`
        Permissions *unsafePermissionsMetadata `json:"permissions"`
    }{}

    // Reading in the request.
    handle, err := os.ReadFile(reqpath)
    if err != nil {
        return fmt.Errorf("failed to read %q; %w", reqpath, err)
    }
    err = json.Unmarshal(handle, &request)
    if err != nil {
        return newHttpError(http.StatusBadRequest, fmt.Errorf("failed to parse JSON from %q; %w", reqpath, err))
    }

    if request.Project == nil {
        return newHttpError(http.StatusBadRequest, fmt.Errorf("expected a 'project' property in %q", reqpath))
    }
    project := *(request.Project)
    err = isBadName(project)
    if err != nil {
        return newHttpError(http.StatusBadRequest, fmt.Errorf("invalid project name; %w", err))
    }
    project_dir := filepath.Join(globals.Registry, project)

    rlock, err := lockDirectoryExclusive(globals.Registry, globals, ctx)
    if err != nil {
        return fmt.Errorf("failed to lock the registry; %w", err)
    }
    defer rlock.Unlock(globals)

    return createProject(project_dir, request.Permissions, req_user)
}

func createProject(project_dir string, inperms *unsafePermissionsMetadata, req_user string) error {
    _, err := os.Stat(project_dir)
    if err == nil {
        return newHttpError(http.StatusBadRequest, fmt.Errorf("project directory %q already exists", project_dir))
    } else if !errors.Is(err, os.ErrNotExist) {
        return fmt.Errorf("failed to stat project directory %q; %w", project_dir, err)
    }

    err = os.MkdirAll(project_dir, 0755)
    if err != nil {
        return fmt.Errorf("failed to create a new project directory %s; %w", project_dir, err)
    }

    perms := permissionsMetadata{}
    if inperms != nil && inperms.Owners != nil {
        perms.Owners = inperms.Owners
    } else {
        perms.Owners = []string{ req_user }
    }
    if inperms != nil && inperms.Uploaders != nil {
        san, err := sanitizeUploaders(inperms.Uploaders)
        if err != nil {
            return newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'permissions.uploaders' in the request details; %w", err))
        }
        perms.Uploaders = san
    } else {
        perms.Uploaders = []uploaderEntry{}
    }
    if inperms != nil && inperms.GlobalWrite != nil {
        perms.GlobalWrite = inperms.GlobalWrite
    }

    err = dumpJson(filepath.Join(project_dir, permissionsFileName), &perms)
    if err != nil {
        return fmt.Errorf("failed to write permissions for %q; %w", project_dir, err)
    }

    // Dumping a mock quota and usage file for consistency with gypsum.
    // Note that the quota isn't actually enforced yet.
    err = os.WriteFile(filepath.Join(project_dir, "..quota"), []byte("{ \"baseline\": 1000000000, \"growth_rate\": 1000000000, \"year\": " + strconv.Itoa(time.Now().Year()) + " }"), 0644)
    if err != nil {
        return fmt.Errorf("failed to write quota for '" + project_dir + "'; %w", err)
    }

    err = os.WriteFile(filepath.Join(project_dir, usageFileName), []byte("{ \"total\": 0 }"), 0644)
    if err != nil {
        return fmt.Errorf("failed to write usage for '" + project_dir + "'; %w", err)
    }

    return nil
}
