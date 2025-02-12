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
    "strconv"
    "strings"
)

func copyFile(src, dest string) error {
    in, err := os.Open(src)
    if err != nil {
        return fmt.Errorf("failed to open input file at '" + src + "'; %w", err)
    }
    defer in.Close()

    out, err := os.OpenFile(dest, os.O_CREATE | os.O_WRONLY, 0644)
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

func readSymlink(path string) (string, error) {
    target, err := os.Readlink(path)
    if err != nil {
        return "", err
    }
    if (!filepath.IsAbs(target)) {
        target = filepath.Clean(filepath.Join(filepath.Dir(path), target))
    }
    return target, nil
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

func createDedupManifest(registry, project, asset string) (map[string]linkMetadata, error) {
    // Loading the latest version's metadata into a deduplication index.
    // There's no need to check for probational versions here as only
    // non-probational versions ever make it into '..latest'.
    last_dedup := map[string]linkMetadata{}
    asset_dir := filepath.Join(registry, project, asset)
    latest_path := filepath.Join(asset_dir, latestFileName)

    _, err := os.Stat(latest_path)
    if err == nil {
        latest, err := readLatest(asset_dir)
        if err != nil {
            return nil, fmt.Errorf("failed to identify the latest version; %w", err)
        }

        manifest, err := readManifest(filepath.Join(asset_dir, latest.Version))
        if err != nil {
            return nil, fmt.Errorf("failed to read the latest version's manifest; %w", err)
        }

        for k, v := range manifest {
            self := linkMetadata{
                Project: project,
                Asset: asset,
                Version: latest.Version,
                Path: k,
            }
            if v.Link != nil {
                if v.Link.Ancestor != nil {
                    self.Ancestor = v.Link.Ancestor
                } else {
                    self.Ancestor = v.Link
                }
            }
            last_dedup[strconv.FormatInt(v.Size, 10) + "-" + v.Md5sum] = self
        }

    } else if !errors.Is(err, os.ErrNotExist) {
        return nil, fmt.Errorf("failed to stat '" + latest_path + "; %w", err)
    }

    return last_dedup, nil
}

/***********************************************
 ***** Links to other registry directories *****
 ***********************************************/

type resolveRegistrySymlinkCache struct {
    Manifest map[string]map[string]manifestEntry
    OnProbation map[string]bool
}

func newResolveRegistrySymlinkCache() *resolveRegistrySymlinkCache {
    return &resolveRegistrySymlinkCache{
        Manifest: map[string]map[string]manifestEntry{},
        OnProbation: map[string]bool{},
    }
}

func resolveRegistrySymlink(
    registry string,
    project string,
    asset string,
    version string,
    target string,
    cache *resolveRegistrySymlinkCache,
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
    prob, ok := cache.OnProbation[key]
    if !ok {
        summary, err := readSummary(filepath.Join(registry, key))
        if err != nil {
            return nil, fmt.Errorf("cannot read the version summary for '" + key + "'; %w", err)
        }
        prob = summary.IsProbational()
        cache.OnProbation[key] = prob
    }
    if prob {
        return nil, errors.New("cannot link to file inside a probational project version asset directory ('" + key + "')")
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
    manifest, ok := cache.Manifest[key]
    if !ok {
        manifest_, err := readManifest(filepath.Join(registry, key))
        if err != nil {
            return nil, fmt.Errorf("cannot read the manifest for '" + key + "'; %w", err)
        }
        manifest = manifest_
        cache.Manifest[key] = manifest
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

func createRegistrySymlink(destination, path, target string) error {
    // We convert the link target to a relative path within the registry so
    // that the registry is easily relocatable.
    working := path
    for {
        working = filepath.Dir(working) 
        if working == "." {
            break
        }
        target = filepath.Join("..", target)
    }

    // Adding three more for the project, asset, version subdirectories.
    target = filepath.Join("..", "..", "..", target) 

    return os.Symlink(target, filepath.Join(destination, path))
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

func createLocalSymlink(destination, path, target string) error {
    working := path
    for {
        working = filepath.Dir(working) 
        if working == "." {
            break
        }
        target = filepath.Join("..", target)
    }
    return os.Symlink(target, filepath.Join(destination, path))
}

/*******************************
 ***** Link metadata store *****
 *******************************/

type linkInfo struct {
    From string
    To string
}

type linkMetadataStore = map[string]map[string]*linkMetadata

func addLinkMetadataStore(path string, link_info *linkMetadata, store linkMetadataStore) {
    subdir, base := filepath.Split(path)
    sublinks, ok := store[subdir]
    if !ok {
        sublinks = map[string]*linkMetadata{}
        store[subdir] = sublinks
    }
    sublinks[base] = link_info
}

func saveLinkMetadataStore(destination string, store linkMetadataStore) error {
    for k, v := range store {
        link_path := filepath.Join(destination, k, linksFileName)
        err := dumpJson(link_path, &v)
        if err != nil {
            return fmt.Errorf("failed to save links for %q; %w", k, err)
        }
    }
    return nil
}

/*********************************************************
 ***** Transfer contents of a non-registry directory *****
 *********************************************************/

func processDirectory(do_transfer bool, source, registry, project, asset, version string, link_whitelist []string) error {
    var last_dedup map[string]linkMetadata 
    var err error
    if do_transfer {
        last_dedup, err = createDedupManifest(registry, project, asset)
        if err != nil {
            return err
        }
    }

    destination := filepath.Join(registry, project, asset, version)
    manifest := map[string]manifestEntry{}
    registry_links := map[string]string{}
    local_links := map[string]string{}
    all_links := linkMetadataStore{}

    // First pass fills the manifest with non-symlink files.
    err = filepath.WalkDir(source, func(src_path string, info fs.DirEntry, err error) error {
        if err != nil {
            return fmt.Errorf("failed to walk into '" + src_path + "'; %w", err)
        }

        base := filepath.Base(src_path)
        if strings.HasPrefix(base, ".") {
            if info.IsDir() {
                return filepath.SkipDir
            } else {
                if !do_transfer && base == linksFileName {
                    err := os.Remove(src_path)
                    if err != nil {
                        return fmt.Errorf("failed to remove old ..links file at %q; %w", src_path, err)
                    }
                }
                return nil
            }
        }

        path, err := filepath.Rel(source, src_path)
        if err != nil {
            return fmt.Errorf("failed to convert %q into a relative path; %w", src_path, err);
        }

        if info.IsDir() {
            if do_transfer {
                err := os.MkdirAll(filepath.Join(destination, path), 0755)
                if err != nil {
                    return fmt.Errorf("failed to create a directory at %q; %w", src_path, err)
                }
            }
            return nil
        }

        restat, err := info.Info()
        if err != nil {
            return fmt.Errorf("failed to stat '" + path + "'; %w", err)
        }

        // Preserving links to targets within the registry, within the 'src' directory, or inside whitelisted directories.
        if restat.Mode() & os.ModeSymlink == os.ModeSymlink {
            target, err := readSymlink(src_path)
            if err != nil {
                return fmt.Errorf("failed to read the symlink at %q; %w", src_path, err)
            }

            target_stat, err := os.Stat(target)
            if err != nil {
                return fmt.Errorf("failed to stat target of link %q; %w", src_path, err)
            }
            if target_stat.IsDir() {
                return fmt.Errorf("target of link %q refers to a directory", src_path)
            }

            local_inside, err := filepath.Rel(source, target)
            if err == nil && filepath.IsLocal(local_inside) {
                local_links[path] = local_inside
                return nil
            }

            registry_inside, err := filepath.Rel(registry, target)
            if err == nil && filepath.IsLocal(registry_inside) {
                registry_links[path] = registry_inside
                return nil
            }

            if isLinkWhitelisted(target, link_whitelist) { // Symlinks to files in whitelisted directories are preserved, but manifest pretends as if they were the files themselves.
                target_sum, err := computeChecksum(target)
                if err != nil {
                    return fmt.Errorf("failed to hash the link target %q; %w", target, err)
                }
                manifest[path] = manifestEntry{
                    Size: target_stat.Size(), 
                    Md5sum: target_sum,
                }
                if do_transfer {
                    final := filepath.Join(destination, path)
                    err := os.Symlink(target, final)
                    if err != nil {
                        return fmt.Errorf("failed to create a symlink for %q to %q; %w", path, target, err)
                    }
                }
                return nil
            } else if !do_transfer {
                return fmt.Errorf("symbolic link %q to file %q outside the registry directory is not allowed", path, target)
            }

            restat = target_stat
        }

        insum, err := computeChecksum(src_path)
        if err != nil {
            return fmt.Errorf("failed to hash the source file; %w", err)
        }

        man_entry := manifestEntry{
            Size: restat.Size(),
            Md5sum: insum,
        }

        if !do_transfer {
            // Seeing if we can create a link to the last version of the file with the same md5sum.
            last_entry, ok := last_dedup[strconv.FormatInt(man_entry.Size, 10) + "-" + man_entry.Md5sum]
            if ok {
                man_entry.Link = &last_entry
                manifest[path] = man_entry
                registry_target := filepath.Join(last_entry.Project, last_entry.Asset, last_entry.Version, last_entry.Path)
                err = createRegistrySymlink(destination, path, registry_target)
                if err != nil {
                    return fmt.Errorf("failed to create a symlink for %q to %q; %w", path, registry_target, err)
                }
                addLinkMetadataStore(path, man_entry.Link, all_links)
                return nil
            }

            // Otherwise we just copy the file.
            final := filepath.Join(destination, path)
            err := copyFile(src_path, final)
            if err != nil {
                return fmt.Errorf("failed to copy file at %q to %q; %w", path, destination, err)
            }
            finalsum, err := computeChecksum(final)
            if err != nil {
                return fmt.Errorf("failed to hash the file at %q; %w", final, err)
            }
            if finalsum != insum {
                return fmt.Errorf("mismatch in checksums between source and destination files for %q", path)
            }
        }

        manifest[path] = man_entry
        return nil
    })
    if err != nil {
        return err
    }

    // Second pass to resolve links to other files in the registry.
    reglink_cache := newResolveRegistrySymlinkCache()

    for path, target := range registry_links {
        tstat, err := os.Stat(filepath.Join(registry, target))
        if err != nil {
            return fmt.Errorf("failed to stat link target %q inside registry; %w", target, err)
        }
        if tstat.IsDir() {
            return fmt.Errorf("symbolic link to registry directory %q is not supported", target)
        }

        obj, err := resolveRegistrySymlink(registry, project, asset, version, target, reglink_cache)
        if err != nil {
            return fmt.Errorf("failed to resolve symlink for %q to registry path %q; %w", path, target, err)
        }
        manifest[path] = *obj
        addLinkMetadataStore(path, obj.Link, all_links)

        if do_transfer {
            err := createRegistrySymlink(destination, path, target)
            if err != nil {
                return fmt.Errorf("failed to create a symlink for %q to registry path %q; %w", path, target, err)
            }
        }
    }

    // Final pass to resolve local links within the newly uploaded directory.
    // We do this last when 'do_transfer = true' to ensure that the link
    // targets actually exist.
    for path, target := range local_links {
        man, err := resolveLocalSymlink(project, asset, version, path, target, local_links, manifest, nil)
        if err != nil {
            return err
        }
        addLinkMetadataStore(path, man.Link, all_links)

        if do_transfer {
            err := createLocalSymlink(destination, path, target)
            if err != nil {
                return fmt.Errorf("failed to create a local symlink from %q to %q; %w", path, target, err)
            }
        }
    }

    // Dumping the JSON metadata.
    manifest_path := filepath.Join(destination, manifestFileName)
    err = dumpJson(manifest_path, &manifest)
    if err != nil {
        return fmt.Errorf("failed to save manifest for %q; %w", destination, err)
    }

    err = saveLinkMetadataStore(destination, all_links)
    return err
}

func transferDirectory(source, registry, project, asset, version string, link_whitelist []string) error {
    return processDirectory(true, source, registry, project, asset, version, link_whitelist)
}

func reindexDirectory(registry, project, asset, version string, link_whitelist []string) error {
    source := filepath.Join(registry, project, asset, version)
    return processDirectory(false, source, registry, project, asset, version, link_whitelist)
}
