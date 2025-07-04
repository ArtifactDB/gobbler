package main

import (
    "fmt"
    "context"
    "path/filepath"
    "os"
    "encoding/json"
    "net/http"
    "time"
)

type validateDirectoryOptions struct {
    LinkWhitelist []string
}

func compareLinksSimple(observed, expected *linkMetadata, what string) error {
    if expected == nil {
        if observed != nil {
            return fmt.Errorf("unexpected %s in manifest", what)
        } 
        return nil
    } else {
        if observed == nil {
            return fmt.Errorf("expected %s in manifest", what)
        } 
    }

    if expected.Project != observed.Project {
        return fmt.Errorf("mismatching %s project in manifest", what)
    }
    if expected.Asset != observed.Asset {
        return fmt.Errorf("mismatching %s asset in manifest", what)
    }
    if expected.Version != observed.Version {
        return fmt.Errorf("mismatching %s version in manifest", what)
    }
    if expected.Path != observed.Path {
        return fmt.Errorf("mismatching %s path in manifest", what)
    }

    return nil
}

func compareLinks(observed, expected *linkMetadata) error {
    err := compareLinksSimple(observed, expected, "link")
    if err != nil {
        return err
    }
    if observed == nil {
        return nil
    }
    return compareLinksSimple(observed.Ancestor, expected.Ancestor, "link ancestor")
}

func validateDirectory(
    registry,
    project,
    asset,
    version string,
    ctx context.Context,
    throttle *concurrencyThrottle,
    options validateDirectoryOptions,
) error {
    source := filepath.Join(registry, project, asset, version)
    old_all_links, err := parseExistingLinkFiles(source, registry, ctx)
    if err != nil {
        return fmt.Errorf("failed to parse existing linkfiles in %q; %w", source, err)
    }

    new_manifest, err := walkDirectory(
        source,
        registry,
        project,
        asset,
        version,
        ctx,
        throttle,
        walkDirectoryOptions{
            Mode: WalkDirectoryValidate,
            Consume: false, // not used during validation, just set for completeness only.
            DeduplicateLatest: nil,
            RestoreLinkParent: createRestoreLinkParentMap(old_all_links),
            IgnoreDot: false,
            LinkWhitelist: options.LinkWhitelist,
        },
    )
    if err != nil {
        return err
    }

    previous_manifest, err := readManifest(source)
    if err != nil {
        return fmt.Errorf("failed to read the manifest at %q; %w", source, err)
    }

    // Checking that everything in the current manifest is also present in the new manifest.
    for path, prev_entry := range previous_manifest {
        new_entry, ok := new_manifest[path]
        if !ok {
            return fmt.Errorf("%q listed in manifest cannot be found in directory %q", path, source)
        }
        if new_entry.Size != prev_entry.Size {
            return fmt.Errorf("incorrect size in manifest for %q in directory %q", path, source)
        }
        if new_entry.Md5sum != prev_entry.Md5sum {
            return fmt.Errorf("incorrect MD5 checksum in manifest for %q in directory %q", path, source)
        }
        err := compareLinks(prev_entry.Link, new_entry.Link)
        if err != nil {
            return fmt.Errorf("mismatching link information for %q in directory %q; %w", path, source, err)
        }
    }

    // Checking that there aren't any new files.
    for path, _ := range new_manifest {
        if _, ok := previous_manifest[path]; !ok {
            return fmt.Errorf("extra file %q is not present in manifest for directory %q", path, source)
        }
    }

    // Now checking that the expected linkfiles exist with the correct contents.
    new_all_links := prepareLinkFiles(new_manifest)
    for lpath, new_links := range new_all_links {
        old_links, ok := old_all_links[lpath]
        if !ok {
            return fmt.Errorf("expected a linkfile at %q in directory %q", lpath, source)
        }

        for fpath, new_entry := range new_links { 
            old_entry, ok := old_links[fpath]
            if !ok {
                return fmt.Errorf("missing path %q from linkfile %q in directory %q", fpath, lpath, source)
            }
            err := compareLinks(old_entry, new_entry)
            if err != nil {
                return fmt.Errorf("mismatching link information for %q in linkefile %q in directory %q; %w", fpath, lpath, source, err)
            }
        }

        for fpath, _ := range old_links { 
            _, ok := new_links[fpath]
            if !ok {
                return fmt.Errorf("extra path %q in linkfile %q in directory %q", fpath, lpath, source)
            }
        }
    }

    // Checking that there aren't any new linkfiles.
    for lpath, _ := range old_all_links {
        if _, ok := new_all_links[lpath]; !ok {
            return fmt.Errorf("extra linkfile %q in directory %q", lpath, source)
        }
    }

    return nil
}

type validateRequest struct {
    Project *string `json:"project"`
    Asset *string `json:"asset"`
    Version *string `json:"version"`
    User string `json:"-"`
}

func validatePreflight(reqpath string) (*validateRequest, error) {
    handle, err := os.ReadFile(reqpath)
    if err != nil {
        return nil, fmt.Errorf("failed to read %q; %w", reqpath, err)
    }

    req_user, err := identifyUser(reqpath)
    if err != nil {
        return nil, fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }

    request := validateRequest{}
    err = json.Unmarshal(handle, &request)
    if err != nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("failed to parse JSON from %q; %w", reqpath, err))
    }

    if request.Project == nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("expected a 'project' property in %q", reqpath))
    }
    project := *(request.Project)
    err = isBadName(project)
    if err != nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("invalid project name %q; %w", project, err))
    }

    if request.Asset == nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("expected an 'asset' property in %q", reqpath))
    }
    asset := *(request.Asset)
    err = isBadName(asset)
    if err != nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("invalid asset name %q; %w", asset, err))
    }

    if request.Version == nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("expected a 'version' property in %q", reqpath))
    }
    version := *(request.Version)
    err = isBadName(version)
    if err != nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("invalid version name %q; %w", version, err))
    }

    request.User = req_user
    return &request, nil
}

func validateHandler(reqpath string, globals *globalConfiguration, ctx context.Context) error {
    request, err := validatePreflight(reqpath)
    if err != nil {
        return err
    }
    project := *(request.Project)
    ok := isAuthorizedToAdmin(request.User, globals.Administrators)
    if !ok {
        return newHttpError(http.StatusForbidden, fmt.Errorf("user '" + request.User + "' is not authorized to validate '" + project + "'"))
    }

    rlock, err := lockDirectoryShared(globals.Registry, globals, ctx)
    if err != nil {
        return fmt.Errorf("failed to acquire the lock on %q; %w", globals.Registry, err)
    }
    defer rlock.Unlock(globals)

    rnnlock, err := lockDirectoryNewDirShared(globals.Registry, globals, ctx)
    if err != nil {
        return fmt.Errorf("failed to acquire the lock on %q; %w", globals.Registry, err)
    }
    defer rnnlock.Unlock(globals)

    project_dir := filepath.Join(globals.Registry, project)
    if err := checkProjectExists(project_dir, project); err != nil {
        return err
    }
    rnnlock.Unlock(globals) // no need for this lock once we know that the project directory exists.

    plock, err := lockDirectoryShared(project_dir, globals, ctx)
    if err != nil {
        return fmt.Errorf("failed to acquire the lock on %q; %w", project_dir, err)
    }
    defer plock.Unlock(globals)

    pnnlock, err := lockDirectoryNewDirShared(project_dir, globals, ctx)
    if err != nil {
        return fmt.Errorf("failed to acquire the lock on %q; %w", project_dir, err)
    }
    defer pnnlock.Unlock(globals)

    asset := *(request.Asset)
    asset_dir := filepath.Join(project_dir, asset)
    if err := checkAssetExists(asset_dir, asset, project); err != nil {
        return err
    }
    pnnlock.Unlock(globals) // no need for this lock once we know that the asset directory exists.

    alock, err := lockDirectoryExclusive(asset_dir, globals, ctx)
    if err != nil {
        return fmt.Errorf("failed to acquire the lock on %q; %w", asset_dir, err)
    }
    defer alock.Unlock(globals)

    version := *(request.Version)
    version_dir := filepath.Join(asset_dir, version)
    if err := checkVersionExists(version_dir, version, asset, project); err != nil {
        return err
    }

    err = validateDirectory(
        globals.Registry,
        project,
        asset,
        version,
        ctx,
        globals.ConcurrencyThrottle,
        validateDirectoryOptions{
            LinkWhitelist: globals.LinkWhitelist,
        },
    )
    if err != nil {
        return fmt.Errorf("failed to validate project; %w", err)
    }

    // Also checking the summary file while we're here.
    summ, err := readSummary(version_dir)
    if err != nil {
        return fmt.Errorf("failed to read the summary file at %q; %w", version_dir, err)
    }
    if summ.UploadUserId == "" {
        return fmt.Errorf("invalid 'upload_user_id' in the summary file at %q", version_dir)
    }
    if _, err := time.Parse(time.RFC3339, summ.UploadStart); err != nil {
        return fmt.Errorf("could not parse 'upload_start' from the summary file at %q; %w", version_dir, err)
    }
    if _, err := time.Parse(time.RFC3339, summ.UploadFinish); err != nil {
        return fmt.Errorf("could not parse 'upload_finish' from the summary file at %q; %w", version_dir, err)
    }

    return nil
}
