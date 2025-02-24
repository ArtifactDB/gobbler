package main

import (
    "os"
    "fmt"
    "encoding/json"
    "path/filepath"
    "strings"
    "net/http"
    "time"
)

type deleteTask struct {
    Project string `json:"project"`
    Asset *string `json:"asset"`
    Version *string `json:"version"`
}

func listToBeDeletedVersions(registry string, to_delete []deleteTask) (map[string]bool, error) {
    version_deleted := map[string]bool{}
    for _, task := range to_delete {
        if task.Asset != nil && task.Version != nil {
            version_deleted[filepath.Join(task.Project, *(task.Asset), *(task.Version))] = true
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
                        version_deleted[filepath.Join(task.Project, aname, vname)] = true
                    }
                }
            }
        }
    }

    return version_deleted, nil
}

func listToBeDeletedFiles(registry string, version_deleted map[string]bool) (map[string]bool, error) {
    lost_files := map[string]bool{}
    for version_dir, _ := range version_deleted {
        full_version_dir := filepath.Join(registry, version_dir)
        man, err := readManifest(full_version_dir)
        if err != nil {
            return nil, fmt.Errorf("failed to read manifest at %q; %w", full_version_dir, err)
        }
        for key, _ := range man {
            lost_files[filepath.Join(version_dir, key)] = true
        }
    }
    return lost_files, nil
}

type rerouteAction struct {
    Copy bool `json:"copy"`
    Path string `json:"path"`
    Source string `json:"source"`
    Usage int64 `json:"usage"`
    Key string `json:"-"`
    Link *linkMetadata `json:"-"`
}

type rerouteProposal struct {
    Actions []rerouteAction
    DeltaManifest map[string]manifestEntry
}

func proposeLinkReroutes(registry string, deleted_files map[string]bool, version_dir string) (*rerouteProposal, error) {
    man, err := readManifest(filepath.Join(registry, version_dir))
    if err != nil {
        return nil, fmt.Errorf("failed to read manifest at %q; %w", version_dir, err)
    }
    manifest_cache := map[string]map[string]manifestEntry{}
    manifest_cache[version_dir] = man

    actions := []rerouteAction{}
    new_man := map[string]manifestEntry{}
    for key, entry := range man {
        if entry.Link == nil {
            continue
        }
        fpath := filepath.Join(version_dir, key)

        parent := filepath.Join(entry.Link.Project, entry.Link.Asset, entry.Link.Version, entry.Link.Path)
        _, lost_parent := deleted_files[parent]
        if entry.Link.Ancestor == nil {
            if lost_parent {
                entry.Link = nil
                new_man[key] = entry
                actions = append(actions, rerouteAction{ 
                    Copy: true, 
                    Source: parent, 
                    Path: fpath, 
                    Key: key,
                    Link: nil, 
                    Usage: entry.Size,
                })
            }
            continue
        }

        ancestor := filepath.Join(entry.Link.Ancestor.Project, entry.Link.Ancestor.Asset, entry.Link.Ancestor.Version, entry.Link.Ancestor.Path)
        _, lost_ancestor := deleted_files[ancestor]
        if !lost_parent && !lost_ancestor {
            continue
        }

        // If either the parent or ancestor was deleted, we need to perform a trace of the ancestry.
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
                    return nil, fmt.Errorf("failed to read manifest at %q; %w", full_target_dir, err)
                }
                target_man = target_man0
            }

            entry, found := target_man[candidate.Path]
            if !found {
                return nil, fmt.Errorf("missing manifest entry for link to %q from %q", target_path, fpath)
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
                actions = append(actions, rerouteAction{ 
                    Copy: true, 
                    Source: parent, 
                    Path: fpath, 
                    Usage: entry.Size,
                    Key: key,
                    Link: nil, 
                })
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
            } else if entry.Link.Ancestor.Project == entry.Link.Project &&
                entry.Link.Ancestor.Asset == entry.Link.Asset &&
                entry.Link.Ancestor.Version == entry.Link.Version &&
                entry.Link.Ancestor.Path == entry.Link.Path { // deleting the ancestor if it's the same as the parent.
                entry.Link.Ancestor = nil
            }

            var reported_src string
            if lost_parent {
                reported_src = parent  // favoring the immediate parent as the reported source.
            } else {
                reported_src = ancestor
            }
            actions = append(actions, rerouteAction{
                Copy: false,
                Source: reported_src,
                Path: fpath,
                Usage: 0,
                Key: key,
                Link: entry.Link,
            })
        }

        new_man[key] = entry
    }

    return &rerouteProposal{ Actions: actions, DeltaManifest: new_man }, nil
}

func executeLinkReroutes(registry string, version_dir string, proposal *rerouteProposal) error {
    delinked := map[string]bool{}
    for _, action := range proposal.Actions {
        dest := filepath.Join(registry, action.Path)
        if action.Copy {
            err := os.Remove(dest)
            if err != nil {
                return fmt.Errorf("failed to remove existing file at %q; %w", dest, err)
            }
            err = copyFile(filepath.Join(registry, action.Source), dest)
            if err != nil {
                return err
            }
            delinked[filepath.Dir(action.Key)] = true
        } else {
            err := createSymlink(dest, registry, action.Link, true)
            if err != nil {
                return err
            }
        }
    }

    // Updating the manifest with the deltas.
    full_version_dir := filepath.Join(registry, version_dir)
    man, err := readManifest(full_version_dir)
    if err != nil {
        return fmt.Errorf("failed to read manifest at %q; %w", full_version_dir, err)
    }
    for k, entry := range proposal.DeltaManifest {
        man[k] = entry
    }
    err = dumpJson(filepath.Join(full_version_dir, manifestFileName), &man)
    if err != nil {
        return err
    }

    // First we get rid of ..links files in directories that might no longer have any links at all;
    // and then we reconstitute all of the link files to be on the safe side.
    for delink, _ := range delinked {
        err := os.Remove(filepath.Join(full_version_dir, delink, linksFileName))
        if err != nil {
            return fmt.Errorf("failed to remove existing ..links file")
        }
    }
    return recreateLinkFiles(full_version_dir, man)
}

func rerouteLinksHandler(reqpath string, globals *globalConfiguration) ([]rerouteAction, error) {
    req_user, err := identifyUser(reqpath)
    if err != nil {
        return nil, fmt.Errorf("failed to find owner of %q; %w", reqpath, err)
    }
    if !isAuthorizedToAdmin(req_user, globals.Administrators) {
        return nil, newHttpError(http.StatusForbidden, fmt.Errorf("user %q is not authorized to delete a project", req_user))
    }

    // First we validate the request.
    all_incoming := struct {
        ToDelete []deleteTask `json:"to_delete"`
        DryRun bool `json:"dry_run"`
    }{}
    contents, err := os.ReadFile(reqpath)
    if err != nil {
        return nil, fmt.Errorf("failed to read %q; %w", reqpath, err)
    }

    err = json.Unmarshal(contents, &all_incoming)
    if err != nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("failed to parse JSON from %q; %w", reqpath, err))
    } else if all_incoming.ToDelete == nil {
        return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("expected a 'to_delete' property in %q; %w", reqpath, err))
    }

    for _, incoming := range all_incoming.ToDelete {
        err := isMissingOrBadName(&(incoming.Project))
        if err != nil {
            return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'project' property in %q; %w", reqpath, err))
        }

        if incoming.Asset == nil {
            if incoming.Version != nil {
                return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("'version' requires the 'asset' to be specified in %q; %w", reqpath, err))
            }
        } else {
            err = isMissingOrBadName(incoming.Asset)
            if err != nil {
                return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'asset' property in %q; %w", reqpath, err))
            }

            if incoming.Version == nil {
                continue
            }
            err = isMissingOrBadName(incoming.Version)
            if err != nil {
                return nil, newHttpError(http.StatusBadRequest, fmt.Errorf("invalid 'version' property in %q; %w", reqpath, err))
            }
        }
    }

    // Obtaining an all-of-registry lock before we identify the rerouting actions.
    err = lockRegistry(globals, 10 * time.Second)
    if err != nil {
        return nil, fmt.Errorf("failed to acquire the lock on the registry; %w", err)
    }
    defer unlockRegistry(globals)

    to_delete_versions, err := listToBeDeletedVersions(globals.Registry, all_incoming.ToDelete)
    if err != nil {
        return nil, err
    }

    to_delete_files, err := listToBeDeletedFiles(globals.Registry, to_delete_versions)
    if err != nil {
        return nil, err
    }

    actions := []rerouteAction{}
    if len(to_delete_files) == 0 {
        return actions, nil
    }

    projects, err := listUserDirectories(globals.Registry)
    if err != nil {
        return nil, err
    }

    // First pass to identify all the rerouting actions across the registry.
    all_changes := map[string]*rerouteProposal{}
    all_usage := map[string]*usageMetadata{}
    dry_run := all_incoming.DryRun

    for _, project := range projects {
        project_dir := filepath.Join(globals.Registry, project)
        assets, err := listUserDirectories(project_dir)
        if err != nil {
            return nil, fmt.Errorf("failed to list assets for project %q; %w", project, err)
        }

        for _, asset := range assets {
            asset_dir := filepath.Join(project_dir, asset)
            versions, err := listUserDirectories(asset_dir)
            if err != nil {
                return nil, fmt.Errorf("failed to list versions for asset %q in project %q; %w", asset, project, err)
            }

            for _, version := range versions {
                version_dir := filepath.Join(project, asset, version)
                if _, found := to_delete_versions[version_dir]; found { // no need to process version directories that are about to be deleted.
                    continue
                }

                cur_changes, err := proposeLinkReroutes(globals.Registry, to_delete_files, version_dir)
                if err != nil {
                    return nil, fmt.Errorf("failed to reroute links for version %q of asset %q in project %q; %w", version, asset, project, err)
                }
                if len(cur_changes.Actions) == 0 {
                    continue
                }
                all_changes[version_dir] = cur_changes

                if !dry_run {
                    cur_usage, ok := all_usage[project]
                    if !ok {
                        usage0, err := readUsage(project_dir)
                        if err != nil {
                            return nil, fmt.Errorf("failed to read usage for %q; %w", project_dir, err)
                        }
                        cur_usage = usage0
                    }
                    for _, action := range cur_changes.Actions {
                        cur_usage.Total += action.Usage
                    }
                    all_usage[project] = cur_usage
                }
            }
        }
    }

    // Second pass to actually implement the changes. This two-pass approach
    // improves the atomicity of the rerouting operation as any failures in the
    // first pass won't leave the registry in a half-mutated state.
    for vpath, info := range all_changes {
        actions = append(actions, (info.Actions)...)
        if dry_run {
            continue
        }
        err := executeLinkReroutes(globals.Registry, vpath, info)
        if err != nil {
            return nil, err
        }
    }

    if !dry_run {
        for project, usage := range all_usage {
            usage_path := filepath.Join(globals.Registry, project, usageFileName)
            err = dumpJson(usage_path, usage)
            if err != nil {
                return nil, fmt.Errorf("failed to save updated usage for project %q; %w", project, err)
            }
        }
    }

    return actions, nil
}
