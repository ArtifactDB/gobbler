package main

import (
    "os"
    "fmt"
    "encoding/json"
    "path/filepath"
    "time"
    "errors"
    "strings"
    "net/http"
)

type deleteTasks struct {
    Project string
    Asset *string
    Version *string
}

func listDeletedFiles(registry string, to_delete []deleteTasks) (map[string]bool, error) {
    version_deleted := []string{}
    for _, task := range to_delete {
        if task.Asset != nil && task.Version != nil {
            version_deleted = append(version_deleted, filepath.Join(task.Project, *(task.Asset), *(task.Version)))
            continue
        }

        project_dir := filepath.Join(registry, task.Project)
        all_assets := []string{}
        if task.Asset != nil {
            all_assets = append(all_assets, *(task.Asset))
        } else {
            asset_listing, err := os.ReadDir(project_dir)
            if err != nil {
                return nil, fmt.Errorf("failed to read contents of %q; %w", project_dir, err)
            }
            for _, asset := range asset_listing {
                if asset.IsDir() {
                    aname := asset.Name()
                    if !strings.HasPrefix(aname, ".") {
                        all_assets = append(all_assets, aname)
                    }
                }
            }
        }

        for _, aname := range all_assets {
            asset_dir := filepath.Join(project_dir, aname)
            version_listing, err := os.ReadDir(asset_dir)
            if err != nil {
                return nil, fmt.Errorf("failed to read contents of %q; %w", asset_dir, err)
            }
            for _, version := range version_listing {
                if version.IsDir() {
                    vname := version.Name()
                    if !strings.HasPrefix(vname, ".") {
                        version_deleted = append(version_deleted, filepath.Join(task.Project, aname, vname))
                    }
                }
            }
        }
    }

    lost_files := map[string]bool{}
    for _, vpath := range version_deleted {
        full_vpath := filepath.Join(registry, vpath)
        man, err := readManifest(full_vpath)
        if err != nil {
            return nil, fmt.Errorf("failed to read manifest at %q; %w", full_vpath, err)
        }
        for key, _ := range man {
            lost_files[filepath.Join(vpath, key)] = true
        }
    }

    return lost_files, nil
}

func copyFileOverwrite(src, dest string) error {
    err := os.Remove(dest)
    if err != nil {
        return fmt.Errorf("failed to remove existing file at %q; %w", dest, err)
    }
    return copyFile(src, dest)
}

func rerouteLinksForVersion(registry string, deleted_files map[string]bool, version_dir string) error {
    man, err := readManifest(filepath.Join(registry, version_dir))
    if err != nil {
        return fmt.Errorf("failed to read manifest at %q; %w", version_dir, err)
    }
    manifest_cache := map[string]map[string]manifestEntry{}
    manifest_cache[version_dir] = man

    new_man := map[string]manifestEntry{}
    for key, entry := range man {
        if entry.Link != nil {
            new_man[key] = entry
            continue
        }
        fpath := filepath.Join(version_dir, key)
        full_path := filepath.Join(registry, fpath)

        parent := filepath.Join(entry.Link.Project, entry.Link.Asset, entry.Link.Version, entry.Link.Path)
        _, lost_parent := deleted_files[parent]
        if entry.Link.Ancestor == nil {
            if lost_parent {
                err = copyFileOverwrite(filepath.Join(registry, parent), full_path)
                if err != nil {
                    return err
                }
                entry.Link = nil
            }
            new_man[key] = entry
            continue
        }

        ancestor := filepath.Join(entry.Link.Ancestor.Project, entry.Link.Ancestor.Asset, entry.Link.Ancestor.Version, entry.Link.Ancestor.Path)
        _, lost_ancestor := deleted_files[ancestor]
        if !lost_parent && !lost_ancestor {
            new_man[key] = entry
            continue
        }

        // Otherwise we fall back to a full trace of the ancestry.
        candidate := entry.Link
        var living_parent *linkMetadata
        var living_ancestor *linkMetadata

        for candidate != nil {
            target_dir := filepath.Join(candidate.Project, candidate.Asset, candidate.Version)
            target_path := filepath.Join(target_dir, candidate.Path)
            if _, found := deleted_files[target_path]; !found {
                if living_parent == nil {
                    living_parent = candidate
                    if !lost_ancestor { // no need to continue recursion if we haven't lost the ancestor.
                        break
                    }
                } else {
                    living_ancestor = candidate
                }
            }

            target_man, found := manifest_cache[target_dir]
            if !found {
                full_target_dir := filepath.Join(registry, target_dir)
                target_man0, err := readManifest(full_target_dir)
                if err != nil {
                    return fmt.Errorf("failed to read manifest at %q; %w", full_target_dir, err)
                }
                target_man = target_man0
            }

            entry, found := target_man[candidate.Path]
            if !found {
                return fmt.Errorf("missing manifest entry for link to %q from %q", target_path, fpath) 
            }
            candidate = entry.Link
        }

        if lost_parent {
            if living_parent != nil {
                entry.Link.Project = living_parent.Project
                entry.Link.Asset = living_parent.Asset
                entry.Link.Version = living_parent.Version
                entry.Link.Path = living_parent.Path
            } else {
                entry.Link = nil
            }
        }

        if entry.Link != nil {
            if lost_ancestor {
                if living_ancestor != nil {
                    entry.Link.Ancestor.Project = living_ancestor.Project
                    entry.Link.Ancestor.Asset = living_ancestor.Asset
                    entry.Link.Ancestor.Version = living_ancestor.Version
                    entry.Link.Ancestor.Path = living_ancestor.Path
                } else {
                    entry.Link.Ancestor = nil
                }
            }
            err := createSymlink(full_path, registry, entry.Link, true)
            if err != nil {
                return err
            }
        } else {
            err = copyFileOverwrite(filepath.Join(registry, parent), full_path)
            if err != nil {
                return err
            }
        }

        man[key] = entry
    }

    return nil
}

func deleteProjectHandler(reqpath string, globals *globalConfiguration) error {
    req_user, err := identifyUser(reqpath)
    if err != nil {
        return fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }
    if !isAuthorizedToAdmin(req_user, globals.Administrators) {
        return newHttpError(http.StatusForbidden, fmt.Errorf("user %q is not authorized to delete a project", req_user))
    }

    incoming := struct {
        Project *string `json:"project"`
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
    }

    project_dir := filepath.Join(globals.Registry, *(incoming.Project))
    if _, err := os.Stat(project_dir); errors.Is(err, os.ErrNotExist) {
        return nil
    }
    err = os.RemoveAll(project_dir)
    if err != nil {
        return fmt.Errorf("failed to delete %s; %v", project_dir, err)
    }

    payload := map[string]string { 
        "type": "delete-project", 
        "project": *(incoming.Project),
    }
    err = dumpLog(globals.Registry, &payload)
    if err != nil {
        return fmt.Errorf("failed to create log for project deletion; %w", err)
    }

    return nil
}

func deleteAssetHandler(reqpath string, globals *globalConfiguration) error {
    req_user, err := identifyUser(reqpath)
    if err != nil {
        return fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }
    if !isAuthorizedToAdmin(req_user, globals.Administrators) {
        return newHttpError(http.StatusForbidden, fmt.Errorf("user %q is not authorized to delete a project", req_user))
    }

    incoming := struct {
        Project *string `json:"project"`
        Asset *string `json:"asset"`
        Force *bool `json:"force"`
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

        err = isMissingOrBadName(incoming.Asset)
        if err != nil {
            return newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'asset' property in %q; %w", reqpath, err))
        }
    }

    force_deletion := incoming.Force != nil && *(incoming.Force)

    project_dir := filepath.Join(globals.Registry, *(incoming.Project))
    if _, err := os.Stat(project_dir); errors.Is(err, os.ErrNotExist) {
        return nil
    }
    err = globals.Locks.LockDirectory(project_dir, 10 * time.Second)
    if err != nil {
        return fmt.Errorf("failed to lock project directory %q; %w", project_dir, err)
    }
    defer globals.Locks.Unlock(project_dir)

    asset_dir := filepath.Join(project_dir, *(incoming.Asset))
    if _, err := os.Stat(asset_dir); errors.Is(err, os.ErrNotExist) {
        return nil
    }
    asset_usage, asset_usage_err := computeAssetUsage(asset_dir)
    if asset_usage_err != nil && !force_deletion {
        return fmt.Errorf("failed to compute usage for %s; %v", asset_dir, asset_usage_err)
    }

    err = os.RemoveAll(asset_dir)
    if err != nil {
        return fmt.Errorf("failed to delete %s; %v", asset_dir, err)
    }

    if asset_usage_err == nil {
        project_usage, err := readUsage(project_dir)
        if err != nil {
            return fmt.Errorf("failed to read usage for %s; %v", project_dir, err)
        }
        project_usage.Total -= asset_usage 
        if project_usage.Total < 0 {
            project_usage.Total = 0
        }
        err = dumpJson(filepath.Join(project_dir, usageFileName), &project_usage)
        if err != nil {
            return fmt.Errorf("failed to update usage for %s; %v", project_dir, err)
        }
    }

    payload := map[string]string { 
        "type": "delete-asset", 
        "project": *(incoming.Project),
        "asset": *(incoming.Asset),
    }
    err = dumpLog(globals.Registry, &payload)
    if err != nil {
        return fmt.Errorf("failed to create log for asset deletion; %w", err)
    }

    return nil
}

func deleteVersionHandler(reqpath string, globals *globalConfiguration) error {
    req_user, err := identifyUser(reqpath)
    if err != nil {
        return fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }
    if !isAuthorizedToAdmin(req_user, globals.Administrators) {
        return newHttpError(http.StatusForbidden, fmt.Errorf("user %q is not authorized to delete a project", req_user))
    }

    incoming := struct {
        Project *string `json:"project"`
        Asset *string `json:"asset"`
        Version *string `json:"version"`
        Force *bool `json:"force"`
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
        err = isMissingOrBadName(incoming.Asset)
        if err != nil {
            return newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'asset' property in %q; %w", reqpath, err))
        }
        err = isMissingOrBadName(incoming.Version)
        if err != nil {
            return newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'version' property in %q; %w", reqpath, err))
        }
    }

    force_deletion := incoming.Force != nil && *(incoming.Force)

    // We lock the project directory as (i) it's convention to lock the entire
    // project even if we're mutating a single asset and (ii) we need to update
    // the usage anyway so we'd have to obtain this lock eventually.
    project_dir := filepath.Join(globals.Registry, *(incoming.Project))
    if _, err := os.Stat(project_dir); errors.Is(err, os.ErrNotExist) {
        return nil
    }
    err = globals.Locks.LockDirectory(project_dir, 10 * time.Second)
    if err != nil {
        return fmt.Errorf("failed to lock project directory %q; %w", project_dir, err)
    }
    defer globals.Locks.Unlock(project_dir)

    asset_dir := filepath.Join(project_dir, *(incoming.Asset))
    if _, err := os.Stat(asset_dir); errors.Is(err, os.ErrNotExist) {
        return nil
    }

    version_dir := filepath.Join(asset_dir, *(incoming.Version))
    if _, err := os.Stat(version_dir); errors.Is(err, os.ErrNotExist) {
        return nil
    }

    version_usage, version_usage_err := computeVersionUsage(version_dir)
    if version_usage_err != nil && !force_deletion {
        return fmt.Errorf("failed to compute usage for %s; %v", version_dir, version_usage_err)
    }

    summ, summ_err := readSummary(version_dir)
    if summ_err != nil && !force_deletion {
        return fmt.Errorf("failed to read summary for %s; %v", version_dir, summ_err)
    }

    err = os.RemoveAll(version_dir)
    if err != nil {
        return fmt.Errorf("failed to delete %s; %v", asset_dir, err)
    }

    if version_usage_err == nil {
        project_usage, err := readUsage(project_dir)
        if err != nil {
            return fmt.Errorf("failed to read usage for %s; %v", project_dir, err)
        }

        project_usage.Total -= version_usage
        if project_usage.Total < 0 {
            project_usage.Total = 0
        }
        err = dumpJson(filepath.Join(project_dir, usageFileName), &project_usage)
        if err != nil {
            return fmt.Errorf("failed to update usage for %s; %v", project_dir, err)
        }
    }

    if summ_err == nil && (summ.OnProbation == nil || !(*summ.OnProbation)) {
        // Only need to make a log if the version is non-probational.
        prev, err := readLatest(asset_dir)
        was_latest := false
        if err == nil {
            was_latest = (prev.Version == *(incoming.Version))
        } else if !errors.Is(err, os.ErrNotExist) {
            return fmt.Errorf("failed to read the latest version for %s; %v", asset_dir, err)
        }

        payload := map[string]interface{} { 
            "type": "delete-version", 
            "project": *(incoming.Project),
            "asset": *(incoming.Asset),
            "version": *(incoming.Version),
            "latest": was_latest,
        }

        err = dumpLog(globals.Registry, &payload)
        if err != nil {
            return fmt.Errorf("failed to create log for version deletion; %w", err)
        }

        // Also refreshing the latest version.
        _, latest_err := refreshLatest(asset_dir)
        if latest_err != nil && !force_deletion {
            return fmt.Errorf("failed to update the latest version for %s; %v", asset_dir, latest_err)
        }
    }

    return nil
}

