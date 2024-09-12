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

func resolveRegistrySymlink(
    registry string,
    project string,
    asset string,
    version string,
    relative_target string,
    manifest_cache map[string]map[string]manifestEntry,
    summary_cache map[string]bool,
) (*manifestEntry, error) {

    fragments := []string{}
    working := relative_target
    for working != "." {
        fragments = append(fragments, filepath.Base(working))
        working = filepath.Dir(working)
    }
    if len(fragments) <= 3 {
        return nil, errors.New("unexpected link to file outside of a project asset version directory ('" + relative_target + "')")
    }

    for _, base := range fragments {
        if strings.HasPrefix(base, "..") {
            return nil, fmt.Errorf("link components cannot refer to internal '..' files (%q)", relative_target)
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
        return nil, errors.New("cannot link to file inside the currently-transferring project asset version directory ('" + relative_target + "')")
    }

    key := filepath.Join(tproject, tasset, tversion)

    // Prohibit links to probational version.
    prob, ok := summary_cache[key]
    if !ok {
        summary, err := readSummary(filepath.Join(registry, key))
        if err != nil {
            return nil, fmt.Errorf("cannot read the version summary for '" + key + "'; %w", err)
        }
        prob = summary.IsProbational()
        summary_cache[key] = prob
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
    manifest, ok := manifest_cache[key]
    if !ok {
        manifest_, err := readManifest(filepath.Join(registry, key))
        if err != nil {
            return nil, fmt.Errorf("cannot read the manifest for '" + key + "'; %w", err)
        }
        manifest = manifest_
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

func createRegistrySymlink(relative_target, relative_link, full_link string) error {
    // We convert the link target to a relative path within the registry so
    // that the registry is easily relocatable.
    working := relative_link
    for {
        working = filepath.Dir(working) 
        if working == "." {
            break
        }
        relative_target = filepath.Join("..", relative_target)
    }

    // Adding three more for the project, asset, version subdirectories.
    relative_target = filepath.Join("..", "..", "..", relative_target) 

    err := os.Symlink(relative_target, full_link)
    if err != nil {
        return fmt.Errorf("failed to create a registry symlink at '" + full_link + "'; %w", err)
    }
    return nil
}

type localLinkInfo struct {
    Target string
    Final string
}

func resolveLocalSymlink(
    project string,
    asset string,
    version string,
    rel string,
    details *localLinkInfo,
    local_links map[string]localLinkInfo,
    manifest map[string]manifestEntry,
    traversed map[string]bool,
    source string,
) (*manifestEntry, error) {
    var target_deets *manifestEntry
    man_deets, man_ok := manifest[details.Target]
    if man_ok {
        target_deets = &man_deets

    } else {
        _, trav_ok := traversed[rel]
        if trav_ok {
            return nil, fmt.Errorf("cyclic symlinks detected at '%s'", filepath.Join(source, rel))
        }
        traversed[rel] = false

        rel_deets, rel_ok := local_links[details.Target]
        if !rel_ok {
            return nil, fmt.Errorf("symlink at '%s' should point to a manifest file or another symlink", filepath.Join(source, rel))
        }

        ancestor, err := resolveLocalSymlink(project, asset, version, details.Target, &rel_deets, local_links, manifest, traversed, source)
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
            Path: details.Target,
        },
    }

    if target_deets.Link != nil {
        if target_deets.Link.Ancestor != nil {
            output.Link.Ancestor = target_deets.Link.Ancestor
        } else {
            output.Link.Ancestor = target_deets.Link
        }
    }

    manifest[rel] = output
    return &output, nil
}

func createLocalSymlink(relative_target, relative_link, full_link string) error {
    working := relative_link
    for {
        working = filepath.Dir(working) 
        if working == "." {
            break
        }
        relative_target = filepath.Join("..", relative_target)
    }

    err := os.Symlink(relative_target, full_link)
    if err != nil {
        return fmt.Errorf("failed to create a local symlink at '" + full_link + "'; %w", err)
    }
    return nil
}

func Transfer(source, registry, project, asset, version string) error {
    last_dedup, err := createDedupManifest(registry, project, asset)
    if err != nil {
        return err
    }

    // Creating a function to add the links.
    links := map[string]map[string]*linkMetadata{}
    addLink := func(rel string, link_info *linkMetadata) {
        subdir, base := filepath.Split(rel)
        sublinks, ok := links[subdir]
        if !ok {
            sublinks = map[string]*linkMetadata{}
            links[subdir] = sublinks
        }
        sublinks[base] = link_info
    }

    type basicSymLink struct {
        Path string
        Rel string
        Final string
    }
    more_links := []basicSymLink{}

    // First pass fills the manifest with non-symlink files.
    destination := filepath.Join(registry, project, asset, version)
    manifest := map[string]manifestEntry{}

    err = filepath.WalkDir(source, func(path string, info fs.DirEntry, err error) error {
        if err != nil {
            return fmt.Errorf("failed to walk into '" + path + "'; %w", err)
        }

        base := filepath.Base(path)
        if strings.HasPrefix(base, ".") {
            if info.IsDir() {
                return filepath.SkipDir
            } else {
                return nil
            }
        }

        rel, err := filepath.Rel(source, path)
        if err != nil {
            return fmt.Errorf("failed to convert '" + path + "' into a relative path; %w", err);
        }

        final := filepath.Join(destination, rel)
        if info.IsDir() {
            err := os.MkdirAll(final, 0755)
            if err != nil {
                return fmt.Errorf("failed to create a destination directory at '" + rel + "'; %w", err)
            }
            return nil
        }

        restat, err := info.Info()
        if err != nil {
            return fmt.Errorf("failed to stat '" + path + "'; %w", err)
        }
        insize := restat.Size()

        // Symlinks to files inside the registry are preserved.
        if restat.Mode() & os.ModeSymlink == os.ModeSymlink {
            more_links = append(more_links, basicSymLink{ Path: path, Rel: rel, Final: final })
            return nil
        }

        insum, err := computeChecksum(path)
        if err != nil {
            return fmt.Errorf("failed to hash the source file; %w", err)
        }

        man_entry := manifestEntry{
            Size: insize,
            Md5sum: insum,
        }

        // Seeing if we can create a link to the last version of the file with the same md5sum.
        last_entry, ok := last_dedup[strconv.FormatInt(man_entry.Size, 10) + "-" + man_entry.Md5sum]
        if ok {
            man_entry.Link = &last_entry
            manifest[rel] = man_entry
            err = createRegistrySymlink(filepath.Join(last_entry.Project, last_entry.Asset, last_entry.Version, last_entry.Path), rel, final)
            if err != nil {
                return fmt.Errorf("failed to create a symlink for '" + rel + "'; %w", err)
            }
            addLink(rel, man_entry.Link)
            return nil
        }

        err = copyFile(path, final)
        if err != nil {
            return fmt.Errorf("failed to copy file at '" + rel + "'; %w", err)
        }

        finalsum, err := computeChecksum(final)
        if err != nil {
            return fmt.Errorf("failed to hash the destination file for '" + rel + "'; %w", err)
        }
        if finalsum != insum {
            return errors.New("mismatch in checksums between source and destination files for '" + rel + "'")
        }

        manifest[rel] = man_entry
        return nil
    })
    if err != nil {
        return err
    }

    // Second pass goes through all the symlinks to existing files in the registry.
    manifest_cache := map[string]map[string]manifestEntry{}
    summary_cache := map[string]bool{}
    local_links := map[string]localLinkInfo{}

    for _, entry := range more_links {
        path := entry.Path
        rel := entry.Rel
        final := entry.Final

        target, err := os.Readlink(path)
        if err != nil {
            return fmt.Errorf("failed to read the symlink at '" + path + "'; %w", err)
        }

        if (!filepath.IsAbs(target)) {
            target = filepath.Clean(filepath.Join(filepath.Dir(path), target))
        }

        registry_inside, err := filepath.Rel(registry, target)
        if err != nil || !filepath.IsLocal(registry_inside) {
            local_inside, err := filepath.Rel(source, target)
            if err != nil || !filepath.IsLocal(local_inside) {
                return fmt.Errorf("symbolic links to files outside the source or registry directories (%q) are not supported", target)
            }
            local_links[rel] = localLinkInfo{ Target: local_inside, Final: final }
            continue
        }

        tstat, err := os.Stat(target)
        if err != nil {
            return fmt.Errorf("failed to stat link target %q; %w", target, err)
        }
        if tstat.IsDir() {
            return fmt.Errorf("symbolic links to directories (%q) are not supported", target)
        }

        obj, err := resolveRegistrySymlink(registry, project, asset, version, registry_inside, manifest_cache, summary_cache)
        if err != nil {
            return fmt.Errorf("failed to resolve the symlink at '" + path + "'; %w", err)
        }
        manifest[rel] = *obj

        err = createRegistrySymlink(registry_inside, rel, final)
        if err != nil {
            return fmt.Errorf("failed to create a symlink for '" + rel + "'; %w", err)
        }
        addLink(rel, obj.Link)
    }

    // Third pass to recursively resolve local symlinks.
    traversed := map[string]bool{}
    for rel, info := range local_links {
        man, err := resolveLocalSymlink(project, asset, version, rel, &info, local_links, manifest, traversed, source)
        if err != nil {
            return err
        }

        err = createLocalSymlink(info.Target, rel, info.Final)
        if err != nil {
            return fmt.Errorf("failed to create a symlink for '" + rel + "'; %w", err)
        }
        addLink(rel, man.Link)
    }

    // Dumping the JSON metadata.
    manifest_path := filepath.Join(destination, manifestFileName)
    err = dumpJson(manifest_path, &manifest)
    if err != nil {
        return fmt.Errorf("failed to save manifest for %q; %w", destination, err)
    }

    for k, v := range links {
        link_path := filepath.Join(destination, k, linksFileName)
        err = dumpJson(link_path, &v)
        if err != nil {
            return fmt.Errorf("failed to save links for %q; %w", k, err)
        }
    }

    return err
}
