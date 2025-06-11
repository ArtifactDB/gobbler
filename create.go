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
)

func createProjectHandler(reqpath string, globals *globalConfiguration) error {
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

    return createProject(project, request.Permissions, req_user, globals)
}

func createProject(project string, inperms *unsafePermissionsMetadata, req_user string, globals *globalConfiguration) error {
    err := isBadName(project)
    if err != nil {
        return newHttpError(http.StatusBadRequest, fmt.Errorf("invalid project name; %w", err))
    }

    rlock, err := lockDirectoryExclusive(globals, globals.Registry)
    if err != nil {
        return fmt.Errorf("failed to lock the registry; %w", err)
    }
    defer rlock.Unlock(globals)

    project_dir := filepath.Join(globals.Registry, project)
    if _, err = os.Stat(project_dir); !errors.Is(err, os.ErrNotExist) {
        return newHttpError(http.StatusBadRequest, fmt.Errorf("project %q already exists", project))
    }

    err = os.MkdirAll(project_dir, 0755)
    if err != nil {
        return fmt.Errorf("failed to create a new project directory for %s; %w", project, err)
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
