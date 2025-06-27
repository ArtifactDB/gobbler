package main

import (
    "crypto/md5"
    "path/filepath"
    "fmt"
    "os"
    "os/user"
    "io"
    "io/fs"
    "encoding/hex"
    "encoding/json"
    "errors"
    "strings"
    "context"
    "sync"
)

func copyFile(src, dest string) error {
    in, err := os.Open(src)
    if err != nil {
        return fmt.Errorf("failed to open input file at '" + src + "'; %w", err)
    }
    defer in.Close()

    out, err := os.OpenFile(dest, os.O_CREATE | os.O_TRUNC | os.O_WRONLY, 0644)
    if err != nil {
        return fmt.Errorf("failed to open output file at '" + dest + "'; %w", err)
    }
    is_closed := false
    defer func() {
        // Don't unconditionally close it, because we need to check
        // whether the close (and thus sync) was successful.
        if !is_closed {
            out.Close()
        }
    }()

    _, err = io.Copy(out, in)
    if err != nil {
        return fmt.Errorf("failed to copy '" + src + "' to '" + dest + "'; %w", err)
    }

    err = out.Close()
    is_closed = true
    if err != nil {
        return fmt.Errorf("failed to close output file at '" + dest + "'; %w", err)
    }

    return nil
}

func computeChecksum(path string) (string, error) {
    in, err_ := os.Open(path)
    if err_ != nil {
        return "", fmt.Errorf("failed to open '" + path + "'; %w", err_)
    }
    defer in.Close()

    h := md5.New()
    _, err := io.Copy(h, in)
    if err != nil {
        return "", fmt.Errorf("failed to hash '" + path + "'; %w", err_)
    }

    return hex.EncodeToString(h.Sum(nil)), nil
}

func fullLinkPath(registry string, link *linkMetadata) string {
    return filepath.Join(registry, link.Project, link.Asset, link.Version, link.Path)
}

func createSymlink(path string, registry string, link *linkMetadata, wipe_existing bool) error {
    if link.Ancestor != nil {
        link = link.Ancestor
    }
    target := fullLinkPath(registry, link)

    // We convert the link target to a relative path within the registry so that the registry is easily relocatable.
    rellocal, err := filepath.Rel(filepath.Dir(path), target)
    if err != nil {
        return fmt.Errorf("failed to determine relative path for registry symlink from %q to %q; %w", path, target, err)
    }

    if wipe_existing {
        if _, err := os.Lstat(path); err == nil || !errors.Is(err, os.ErrNotExist) {
            err = os.Remove(path)
            if err != nil {
                return fmt.Errorf("failed to remove existing symlink at %q; %w", path, err)
            }
        }
    }

    err = os.Symlink(rellocal, path)
    if err != nil {
        return fmt.Errorf("failed to create a symlink from %q to %q; %w", path, target, err)
    }
    return nil
}

func checkRelativeSymlink(parent string, path string, target string) error {
    if filepath.IsAbs(target) {
        return fmt.Errorf("expected a relative path for the link target of %s", path)
    }

    rel, err := filepath.Rel(parent, filepath.Dir(path))
    if err != nil {
        return err
    }

    combined := filepath.Clean(filepath.Join(rel, target))
    if !filepath.IsLocal(combined) {
        return fmt.Errorf("symlink target %q at %q is not local to %q", target, path, parent)
    }

    return nil
}

/***********************************************
 ***** Links to other registry directories *****
 ***********************************************/

func resolveRegistrySymlink(
    registry string,
    project string,
    asset string,
    version string,
    target string,
    manifest_cache map[string]map[string]manifestEntry,
    manifest_cache_lock *sync.Mutex,
    probation_cache map[string]bool,
    probation_cache_lock *sync.Mutex,
) (*manifestEntry, error) {

    fragments := []string{}
    working := target
    for working != "." {
        fragments = append(fragments, filepath.Base(working))
        working = filepath.Dir(working)
    }
    if len(fragments) <= 3 {
        return nil, errors.New("unexpected link to file outside of a project asset version directory ('" + target + "')")
    }

    for _, base := range fragments {
        if strings.HasPrefix(base, "..") {
            return nil, fmt.Errorf("link components cannot refer to internal '..' files (%q)", target)
        }
    }

    for i := 0; i < len(fragments) / 2; i++ {
        o := len(fragments) - i - 1
        fragments[i], fragments[o] = fragments[o], fragments[i]
    }

    tproject := fragments[0]
    tasset := fragments[1]
    tversion := fragments[2]
    if tproject == project && tasset == asset && tversion == version {
        return nil, errors.New("cannot link to file inside the currently-transferring project asset version directory ('" + target + "')")
    }

    key := filepath.Join(tproject, tasset, tversion)

    // Prohibit links to probational version.
    err := func() error {
        probation_cache_lock.Lock()
        defer probation_cache_lock.Unlock()
        prob, ok := probation_cache[key]
        if !ok {
            summary, err := readSummary(filepath.Join(registry, key))
            if err != nil {
                return fmt.Errorf("cannot read the version summary for '" + key + "'; %w", err)
            }
            prob = summary.IsProbational()
            probation_cache[key] = prob
        }
        if prob {
            return errors.New("cannot link to file inside a probational project version asset directory ('" + key + "')")
        }
        return nil
    }()
    if err != nil {
        return nil, err
    }

    tpath := filepath.Join(fragments[3:]...)
    output := manifestEntry{
        Link: &linkMetadata{
            Project: tproject,
            Asset: tasset,
            Version: tversion,
            Path: tpath,
        },
    }

    // Pulling out the size and MD5 checksum of our target path from the manifest.
    manifest_cache_lock.Lock()
    defer manifest_cache_lock.Unlock()
    manifest, ok := manifest_cache[key]
    if !ok {
        manifest0, err := readManifest(filepath.Join(registry, key))
        if err != nil {
            return nil, fmt.Errorf("cannot read the manifest for '" + key + "'; %w", err)
        }
        manifest = manifest0
        manifest_cache[key] = manifest
    }

    found, ok := manifest[tpath]
    if !ok {
        return nil, errors.New("could not find link target '" + tpath + "' in the manifest of '" + key + "'")
    }
    output.Size = found.Size
    output.Md5sum = found.Md5sum

    // Check if our target is itself a link to something else.
    if found.Link != nil {
        if found.Link.Ancestor != nil {
            output.Link.Ancestor = found.Link.Ancestor
        } else {
            output.Link.Ancestor = found.Link
        }
    }

    return &output, nil
}

/***********************************************************
 ***** Local links within the directory being uploaded *****
 ***********************************************************/

func resolveLocalSymlink(
    project string,
    asset string,
    version string,
    path string,
    target string,
    local_links map[string]string,
    manifest map[string]manifestEntry,
    traversed map[string]bool,
) (*manifestEntry, error) {

    var target_deets *manifestEntry
    man_deets, man_ok := manifest[target]
    if man_ok {
        target_deets = &man_deets
    } else {
        if traversed != nil {
            _, trav_ok := traversed[path]
            if trav_ok { // just a second line of defense; normally, cyclic links would have already been detected by a Stat().
                return nil, fmt.Errorf("cyclic symlinks detected at %q", path)
            }
            traversed[path] = false
        } else {
            traversed = map[string]bool{}
        }

        next_target, next_ok := local_links[target]
        if !next_ok {
            return nil, fmt.Errorf("symlink at %q should point to a file in the manifest or another symlink", target)
        }

        ancestor, err := resolveLocalSymlink(project, asset, version, target, next_target, local_links, manifest, traversed)
        if err != nil {
            return nil, err
        }

        target_deets = ancestor
    }

    output := manifestEntry{
        Size: target_deets.Size,
        Md5sum: target_deets.Md5sum,
        Link: &linkMetadata{
            Project: project,
            Asset: asset,
            Version: version,
            Path: target,
        },
    }

    if target_deets.Link != nil {
        if target_deets.Link.Ancestor != nil {
            output.Link.Ancestor = target_deets.Link.Ancestor
        } else {
            output.Link.Ancestor = target_deets.Link
        }
    }

    // Modifying the manifest so that if multiple symlinks have the same target
    // that is also a symlink, only the first call to this function will
    // recurse; all others will just use the cached ancestor in the manifest.
    manifest[path] = output
    return &output, nil
}

/*********************************************************
 ***** Transfer contents of a non-registry directory *****
 *********************************************************/

type WalkDirectoryMode int

const (
    WalkDirectoryTransfer WalkDirectoryMode = iota
    WalkDirectoryReindex
    WalkDirectoryValidate
)

type walkDirectoryOptions struct {
    Mode WalkDirectoryMode
    Consume bool
    DeduplicateLatest map[string]*linkMetadata
    RestoreLinkParent map[string]*linkMetadata
    IgnoreDot bool
    LinkWhitelist []string
}

func walkDirectory(
    source,
    registry,
    project,
    asset,
    version string,
    ctx context.Context,
    throttle *concurrencyThrottle,
    options walkDirectoryOptions,
) (map[string]manifestEntry, error) {

    destination := filepath.Join(registry, project, asset, version)
    manifest := map[string]manifestEntry{}
    transferrable := []string{}
    local_links := map[string]string{}

    var manifest_lock, local_links_lock, transferrable_lock sync.Mutex
    var error_lock sync.RWMutex
    all_errors := []error{}
    safeCheckError := func() error {
        error_lock.RLock()
        defer error_lock.RUnlock()
        if len(all_errors) > 0 {
            return all_errors[0]
        } else {
            return nil
        }
    }
    safeAddError := func(err error) {
        error_lock.Lock()
        defer error_lock.Unlock()
        all_errors = append(all_errors, err)
    }

    var wg sync.WaitGroup
    defer wg.Wait()

    do_transfer := options.Mode == WalkDirectoryTransfer
    do_create_symlink := do_transfer || options.Mode == WalkDirectoryReindex
    do_replace_symlink := options.Mode == WalkDirectoryReindex
    do_check_relative_symlink := options.Mode == WalkDirectoryValidate

    manifest_cache := map[string]map[string]manifestEntry{}
    probation_cache := map[string]bool{}
    var manifest_cache_lock, probation_cache_lock sync.Mutex

    /*** First pass examines all files and decides what to do with them. ***/
    err := filepath.WalkDir(source, func(src_path string, info fs.DirEntry, err error) error {
        if err != nil {
            return fmt.Errorf("failed to walk into '" + src_path + "'; %w", err)
        }

        err = ctx.Err()
        if err != nil {
            return fmt.Errorf("directory processing cancelled; %w", err)
        }

        err = safeCheckError()
        if err != nil {
            return err
        }

        base := filepath.Base(src_path)
        if strings.HasPrefix(base, ".") {
            if options.IgnoreDot || strings.HasPrefix(base, "..") {
                if info.IsDir() {
                    return filepath.SkipDir
                } else {
                    return nil
                }
            }
        }

        rel_path, err := filepath.Rel(source, src_path)
        if err != nil {
            return fmt.Errorf("failed to convert %q into a relative path; %w", src_path, err);
        }

        if info.IsDir() {
            if do_transfer {
                err := os.MkdirAll(filepath.Join(destination, rel_path), 0755)
                if err != nil {
                    return fmt.Errorf("failed to create a directory at %q; %w", src_path, err)
                }
            }
            return nil
        }

        handle := throttle.Wait()
        wg.Add(1);
        go func() {
            defer throttle.Release(handle)
            defer wg.Done();
            err := func() error {
                // Re-check for early cancellation once we get into the goroutine, as throttling might have blocked an arbitrarily long time.
                err := ctx.Err()
                if err != nil {
                    return fmt.Errorf("directory processing cancelled; %w", err)
                }

                err = safeCheckError()
                if err != nil {
                    return err
                }

                restat, err := info.Info()
                if err != nil {
                    return fmt.Errorf("failed to stat '" + src_path + "'; %w", err)
                }

                // Preserving links to targets within the registry, within the 'src' directory, or inside whitelisted directories.
                if restat.Mode() & os.ModeSymlink == os.ModeSymlink {
                    target, err := os.Readlink(src_path)
                    if err != nil {
                        return fmt.Errorf("failed to read the symlink at %q; %w", src_path, err)
                    }

                    original_target := target
                    if (!filepath.IsAbs(target)) {
                        target = filepath.Clean(filepath.Join(filepath.Dir(src_path), target))
                    }
                    if options.RestoreLinkParent != nil {
                        found, ok := options.RestoreLinkParent[rel_path]
                        if ok {
                            parent := fullLinkPath(registry, found)
                            if found.Ancestor != nil && target == fullLinkPath(registry, found.Ancestor) {
                                target = parent
                            } else if target != parent {
                                return fmt.Errorf("unexpected symbolic link to non-ancestor, non-parent file %q from %q", target, src_path)
                            }
                        }
                    }

                    target_stat, err := os.Stat(target)
                    if err != nil {
                        return fmt.Errorf("failed to stat target of link %q; %w", src_path, err)
                    }
                    if target_stat.IsDir() {
                        return fmt.Errorf("target of link %q is a directory", src_path)
                    }

                    local_inside, err := filepath.Rel(source, target)
                    if err == nil && filepath.IsLocal(local_inside) {
                        if do_check_relative_symlink {
                            err := checkRelativeSymlink(destination, src_path, original_target)
                            if err != nil {
                                return err
                            }
                        }
                        local_links_lock.Lock()
                        defer local_links_lock.Unlock()
                        local_links[rel_path] = local_inside
                        return nil
                    }

                    registry_inside, err := filepath.Rel(registry, target)
                    if err == nil && filepath.IsLocal(registry_inside) {
                        if do_check_relative_symlink {
                            err := checkRelativeSymlink(registry, src_path, original_target)
                            if err != nil {
                                return err
                            }
                        }

                        obj, err := resolveRegistrySymlink(
                            registry,
                            project,
                            asset,
                            version,
                            registry_inside,
                            manifest_cache,
                            &manifest_cache_lock,
                            probation_cache,
                            &probation_cache_lock,
                        )
                        if err != nil {
                            return fmt.Errorf("failed to resolve symlink for %q to registry path %q; %w", src_path, target, err)
                        }

                        if do_create_symlink {
                            err = createSymlink(filepath.Join(destination, rel_path), registry, obj.Link, do_replace_symlink)
                            if err != nil {
                                return err
                            }
                        }

                        manifest_lock.Lock()
                        defer manifest_lock.Unlock()
                        manifest[rel_path] = *obj
                        return nil
                    }

                    // Symlinks to files in whitelisted directories are preserved, but manifest pretends as if they were the files themselves.
                    if isLinkWhitelisted(target, options.LinkWhitelist) {
                        target_sum, err := computeChecksum(target)
                        if err != nil {
                            return fmt.Errorf("failed to hash the link target %q; %w", target, err)
                        }

                        manifest_lock.Lock()
                        defer manifest_lock.Unlock()
                        manifest[rel_path] = manifestEntry{ Size: target_stat.Size(), Md5sum: target_sum }

                        if do_transfer {
                            final := filepath.Join(destination, rel_path)
                            err := os.Symlink(target, final)
                            if err != nil {
                                return err
                            }
                        }
                        return nil
                    } else {
                        return fmt.Errorf("symbolic link %q to file %q outside the registry directory is not allowed", rel_path, target)
                    }
                }

                insum, err := computeChecksum(src_path)
                if err != nil {
                    return fmt.Errorf("failed to hash the source file; %w", err)
                }
                man_entry := manifestEntry{ Size: restat.Size(), Md5sum: insum }

                if do_transfer {
                    var link_target *linkMetadata
                    if options.DeduplicateLatest != nil {
                        // Seeing if we can create a link to the last version's file with the same md5sum.
                        last_entry, ok := options.DeduplicateLatest[deduplicateLatestKey(man_entry.Size, man_entry.Md5sum)]
                        if ok {
                            link_target = last_entry
                        }
                    }

                    if link_target == nil {
                        transferrable_lock.Lock()
                        defer transferrable_lock.Unlock()
                        transferrable = append(transferrable, rel_path)
                    } else {
                        man_entry.Link = link_target 
                        err := createSymlink(filepath.Join(destination, rel_path), registry, link_target, false)
                        if err != nil {
                            return err
                        }
                    }
                }

                manifest_lock.Lock()
                defer manifest_lock.Unlock()
                manifest[rel_path] = man_entry
                return nil
            }()

            if err != nil {
                safeAddError(err)
            }
        }()

        return nil
    })
    if err != nil {
        return nil, err
    }

    wg.Wait()
    if len(all_errors) > 0 {
        return nil, all_errors[0]
    }

    /*** 
     *** Second pass performs a copy/move of files.
     *** We use a second pass so any move doesn't break local links within the staging directory during the first pass.
     ***/
    if do_transfer {
        for _, path := range transferrable {
            err := ctx.Err()
            if err != nil {
                return nil, fmt.Errorf("directory processing cancelled; %w", err)
            }

            err = safeCheckError()
            if err != nil {
                return nil, err
            }

            handle := throttle.Wait()
            wg.Add(1);
            go func() {
                defer throttle.Release(handle)
                defer wg.Done();
                err := func() error {
                    // Re-check for early cancellation once we get into the goroutine, as throttling might have blocked an arbitrarily long time.
                    err := ctx.Err()
                    if err != nil {
                        return fmt.Errorf("directory processing cancelled; %w", err)
                    }

                    err = safeCheckError()
                    if err != nil {
                        return err
                    }

                    final := filepath.Join(destination, path)
                    src_path := filepath.Join(source, path)

                    // Setting 'consume=true' ensures that we don't have two copies of all files in the staging and registry at once.
                    // This transparently reduces storage consumption during the upload. 
                    // We first try to set global permissions and rename, otherwise falling back to copy-and-delete.
                    if options.Consume {
                        self, err := user.Current()
                        if err != nil {
                            return fmt.Errorf("failed to identify current user; %w", err)
                        }
                        owner, err := identifyUser(src_path)
                        if err != nil {
                            return fmt.Errorf("failed to identify owner of %q; %w", src_path, err)
                        }
                        if owner == self.Username { // check for correct ownership, otherwise a move is not directly equivalent to copy-and-delete.
                            if err := os.Chmod(src_path, 0644); err == nil {
                                if err := os.Rename(src_path, final); err == nil {
                                    return nil 
                                }
                            }
                        }
                    }

                    err = copyFile(src_path, final)
                    if err != nil {
                        return fmt.Errorf("failed to copy file at %q to %q; %w", path, destination, err)
                    }

                    finalsum, err := computeChecksum(final)
                    if err != nil {
                        return fmt.Errorf("failed to hash the file at %q; %w", final, err)
                    }

                    insum := manifest[path].Md5sum
                    if finalsum != insum {
                        return fmt.Errorf("mismatch in checksums between source and destination files for %q", path)
                    }

                    // We use a copy-and-delete to mimic a move to ensure that our permissions of the new file are configured correctly.
                    // Otherwise we might end up preserving the wrong permissions (especially ownership) of the moved file.
                    // This obviously comes at the cost of some performance but I don't see another way.
                    if options.Consume {
                        os.Remove(src_path)
                    }

                    return nil
                }()

                if err != nil {
                    safeAddError(err)
                }
            }()
        }

        wg.Wait()
        if len(all_errors) > 0 {
            return nil, all_errors[0]
        }
    }

    /*** 
     *** Final pass to resolve local links within the newly uploaded directory. 
     *** Don't try to parallelize this, as different local_links might have the same ancestors.
     *** This would result in repeated attempts to create the same symbolic links from multiple goroutines.
     ***/
    for path, target := range local_links {
        err := ctx.Err()
        if err != nil {
            return nil, fmt.Errorf("directory processing cancelled; %w", err)
        }

        man, err := resolveLocalSymlink(project, asset, version, path, target, local_links, manifest, nil)
        if err != nil {
            return nil, err
        }

        if do_create_symlink {
            err = createSymlink(filepath.Join(destination, path), registry, man.Link, do_replace_symlink)
            if err != nil {
                return nil, err
            }
        }
    }

    wg.Wait()
    if len(all_errors) > 0 {
        return nil, all_errors[0]
    }

    return manifest, nil
}

/******************************************
 ***** Recreate links once we're done *****
 ******************************************/

func readLinkfile(path string) (map[string]*linkMetadata, error) { 
    contents, err := os.ReadFile(path)
    if err != nil {
        return nil, err
    }

    links := map[string]*linkMetadata{}
    err = json.Unmarshal(contents, &links)
    if err != nil {
        return nil, err
    }

    return links, nil
}

func parseExistingLinkFiles(source, registry string, ctx context.Context) (map[string]map[string]*linkMetadata, error) {
    all_links := map[string]map[string]*linkMetadata{}

    err := filepath.WalkDir(source, func(src_path string, info fs.DirEntry, err error) error {
        if err != nil {
            return fmt.Errorf("failed to walk into '" + src_path + "'; %w", err)
        }
        err = ctx.Err()
        if err != nil {
            return fmt.Errorf("directory processing cancelled; %w", err)
        }

        base := filepath.Base(src_path)
        if !strings.HasPrefix(base, "..") {
            return nil
        }

        if info.IsDir() {
            return filepath.SkipDir
        }

        if base != linksFileName {
            return nil
        }

        links, err := readLinkfile(src_path)
        if err != nil {
            return fmt.Errorf("failed to read linkfile at %q; %w", src_path, err)
        }

        for key, link_to := range links {
            if link_to == nil {
                return fmt.Errorf("unexpected nil value for %q in linkfile at %q", key, src_path)
            }
        }

        rel_path, err := filepath.Rel(source, src_path)
        if err != nil {
            return fmt.Errorf("failed to convert %q into a relative path; %w", src_path, err);
        }

        all_links[filepath.Dir(rel_path)] = links
        return nil
    })

    return all_links, err
}

func createRestoreLinkParentMap(all_links map[string]map[string]*linkMetadata) map[string]*linkMetadata {
    link_reroutes := map[string]*linkMetadata{}
    for rel_dir, links := range all_links {
        for path_from, link_to := range links {
            link_reroutes[filepath.Join(rel_dir, path_from)] = link_to
        }
    }
    return link_reroutes
}

func prepareLinkFiles(manifest map[string]manifestEntry) map[string]map[string]*linkMetadata {
    all_links := map[string]map[string]*linkMetadata{}
    for path, entry := range manifest {
        if entry.Link != nil {
            subdir := filepath.Dir(path)
            sublinks, ok := all_links[subdir]
            if !ok {
                sublinks = map[string]*linkMetadata{}
                all_links[subdir] = sublinks
            }
            sublinks[filepath.Base(path)] = entry.Link
        }
    }
    return all_links
}

func recreateLinkFiles(destination string, manifest map[string]manifestEntry) (map[string]map[string]*linkMetadata, error) {
    all_links := prepareLinkFiles(manifest)
    for k, v := range all_links {
        link_path := filepath.Join(destination, k, linksFileName)
        err := dumpJson(link_path, &v)
        if err != nil {
            return nil, fmt.Errorf("failed to save links for %q; %w", k, err)
        }
    }
    return all_links, nil
}
