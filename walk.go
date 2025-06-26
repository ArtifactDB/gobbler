package main

import (
    "crypto/md5"
    "path/filepath"
    "fmt"
    "os"
    "io"
    "io/fs"
    "encoding/hex"
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

func processSymlink(path string, registry string, link *linkMetadata, process_symlink func(string, string) error) error {
    if link.Ancestor != nil {
        link = link.Ancestor
    }
    target := filepath.Join(registry, link.Project, link.Asset, link.Version, link.Path)

    // We convert the link target to a relative path within the registry so that the registry is easily relocatable.
    rellocal, err := filepath.Rel(filepath.Dir(path), target)
    if err != nil {
        return fmt.Errorf("failed to determine relative path for registry symlink from %q to %q; %w", path, target, err)
    }

    return process_symlink(path, rellocal)
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

type walkDirectoryOptions struct {
    Transfer bool
    Consume bool
    IgnoreDot bool
    LinkWhitelist []string
}

func walkDirectory(
    source,
    registry,
    project,
    asset,
    version string,
    process_symlink func(string, string) error,
    reroute_symlink func(string, string) string,
    check_duplicates func(string, manifestEntry) *linkMetadata,
    ctx context.Context,
    throttle *concurrencyThrottle,
    options walkDirectoryOptions,
) (map[string]manifestEntry, error) {

    destination := filepath.Join(registry, project, asset, version)
    manifest := map[string]manifestEntry{}
    transferrable := []string{}
    registry_links := map[string]string{}
    local_links := map[string]string{}

    var manifest_lock, registry_links_lock, local_links_lock, transferrable_lock sync.Mutex
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
            if options.Transfer {
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
                    if (!filepath.IsAbs(target)) {
                        target = filepath.Clean(filepath.Join(filepath.Dir(src_path), target))
                    }
                    target = reroute_symlink(rel_path, target)

                    target_stat, err := os.Stat(target)
                    if err != nil {
                        return fmt.Errorf("failed to stat target of link %q; %w", src_path, err)
                    }
                    if target_stat.IsDir() {
                        return fmt.Errorf("target of link %q is a directory", src_path)
                    }

                    local_inside, err := filepath.Rel(source, target)
                    if err == nil && filepath.IsLocal(local_inside) {
                        local_links_lock.Lock()
                        defer local_links_lock.Unlock()
                        local_links[rel_path] = local_inside
                        return nil
                    }

                    registry_inside, err := filepath.Rel(registry, target)
                    if err == nil && filepath.IsLocal(registry_inside) {
                        registry_links_lock.Lock()
                        defer registry_links_lock.Unlock()
                        registry_links[rel_path] = registry_inside
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

                        if options.Transfer {
                            final := filepath.Join(destination, rel_path)
                            // No need to use process_symlink here, as this can only happen if Transfer = true, in which case we must create the symlink.
                            err := createSymlink(final, target)
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

                if options.Transfer {
                    link_target := check_duplicates(rel_path, man_entry)
                    if link_target == nil {
                        transferrable_lock.Lock()
                        defer transferrable_lock.Unlock()
                        transferrable = append(transferrable, rel_path)
                    } else {
                        man_entry.Link = link_target 
                        // No need to use process_symlink here, as we must create the symlink for Transfer = true.
                        err := processSymlink(filepath.Join(destination, rel_path), registry, link_target, createSymlink)
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

    /*** Second pass performs a copy/move of files. ***/
    if options.Transfer {
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

                    // The move ensures that we don't have two copies of all files in the staging and registry at once.
                    // This reduces storage consumption during the upload. 
                    //
                    // We use a copy-and-delete to mimic a move to ensure that our permissions of the new file are configured correctly.
                    // Otherwise we might end up preserving the wrong permissions (especially ownership) of the moved file.
                    // This obviously comes at the cost of some performance but I don't see another way.
                    //
                    // We use a second pass so this deletion doesn't break local links within the staging directory during the first pass.
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

    /*** Third pass to resolve links to other files in the registry. **/
    manifest_cache := map[string]map[string]manifestEntry{}
    probation_cache := map[string]bool{}
    var manifest_cache_lock, probation_cache_lock sync.Mutex

    for path, target := range registry_links {
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

                tstat, err := os.Stat(filepath.Join(registry, target))
                if err != nil {
                    return fmt.Errorf("failed to stat link target %q inside registry; %w", target, err)
                }
                if tstat.IsDir() {
                    return fmt.Errorf("symbolic link to registry directory %q is not supported", target)
                }

                obj, err := resolveRegistrySymlink(registry, project, asset, version, target, manifest_cache, &manifest_cache_lock, probation_cache, &probation_cache_lock)
                if err != nil {
                    return fmt.Errorf("failed to resolve symlink for %q to registry path %q; %w", path, target, err)
                }
                manifest[path] = *obj

                err = processSymlink(filepath.Join(destination, path), registry, obj.Link, process_symlink)
                if err != nil {
                    return err
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

    /*** Final pass to resolve local links within the newly uploaded directory. ***/
    for path, target := range local_links {
        err := ctx.Err()
        if err != nil {
            return nil, fmt.Errorf("directory processing cancelled; %w", err)
        }

        err = safeCheckError()
        if err != nil {
            return nil, err
        }

        // Don't try to parallelize this, as different local_links might have the same ancestors.
        // This would result in repeated attempts to create the same symbolic links from multiple goroutines.
        man, err := resolveLocalSymlink(project, asset, version, path, target, local_links, manifest, nil)
        if err != nil {
            return nil, err
        }

        err = processSymlink(filepath.Join(destination, path), registry, man.Link, process_symlink)
        if err != nil {
            return nil, err
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

func createSymlink(from, to string) error { 
    err := os.Symlink(from, to)
    if err != nil {
        return fmt.Errorf("failed to create a registry symlink for %q to %q; %w", from, to, err)
    }
    return nil
}

func forceCreateSymlink(from, to string) error {
    if _, err := os.Lstat(from); err == nil || !errors.Is(err, os.ErrNotExist) {
        err = os.Remove(from)
        if err != nil {
            return fmt.Errorf("failed to remove existing symlink at %q; %w", from, err)
        }
    }
    return createSymlink(from, to)
}

func recreateLinkFiles(destination string, manifest map[string]manifestEntry) (map[string]map[string]*linkMetadata, error) {
    all_links := map[string]map[string]*linkMetadata{}
    for path, entry := range manifest {
        if entry.Link != nil {
            subdir, base := filepath.Split(path)
            sublinks, ok := all_links[subdir]
            if !ok {
                sublinks = map[string]*linkMetadata{}
                all_links[subdir] = sublinks
            }
            sublinks[base] = entry.Link
        }
    }

    for k, v := range all_links {
        link_path := filepath.Join(destination, k, linksFileName)
        err := dumpJson(link_path, &v)
        if err != nil {
            return nil, fmt.Errorf("failed to save links for %q; %w", k, err)
        }
    }

    return all_links, nil
}
