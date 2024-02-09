package main

import (
    "crypto/md5"
    "path/filepath"
    "fmt"
    "os"
    "io"
    "io/fs"
    "encoding/hex"
    "encoding/json"
    "errors"
    "strconv"
)

func copy_file(src, dest string) error {
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

func compute_checksum(path string) (string, error) {
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

func resolve_symlink(
    registry string,
    project string,
    asset string,
    version string,
    relative_target string,
    manifest_cache map[string]map[string]ManifestEntry,
    summary_cache map[string]bool,
) (*ManifestEntry, error) {

    fragments := filepath.SplitList(relative_target)
    if len(fragments) <= 3 {
        return nil, errors.New("unexpected link to file outside of a project asset version directory ('" + relative_target + "')")
    }

    if fragments[0] == project && fragments[1] == asset && fragments[2] == version {
        return nil, errors.New("cannot link to file inside the currently-transferring project asset version directory ('" + relative_target + "')")
    }

    key := filepath.Join(fragments[0:3]...)

    // Technically we don't have support for probation yet, 
    // but I'll add it here just in case I forget later.
    prob, ok := summary_cache[key]
    if !ok {
        summary_raw, err := os.ReadFile(filepath.Join(registry, key, "..summary"))
        if err != nil {
            return nil, fmt.Errorf("cannot read the summary file for '" + key + "'; %w", err)
        }

        info := struct { OnProbation bool `json:"on_probation"` }{ OnProbation: false }
        err = json.Unmarshal(summary_raw, &info)
        if err != nil {
            return nil, fmt.Errorf("cannot parse the summary file for '" + key + "'; %w", err)
        }

        prob = info.OnProbation
        summary_cache[key] = prob
    }
    if prob {
        return nil, errors.New("cannot link to file inside a probational project version asset directory ('" + key + "')")
    }

    tpath := filepath.Join(fragments[3:]...)
    output := ManifestEntry{
        Link: &LinkMetadata{
            Project: fragments[0],
            Asset: fragments[1],
            Version: fragments[2],
            Path: tpath,
        },
    }

    // Pulling out the size and MD5 checksum of our target path from the manifest.
    manifest, ok := manifest_cache[key]
    if !ok {
        manifest, err := ReadManifest(filepath.Join(registry, key))
        if err != nil {
            return nil, fmt.Errorf("cannot read the manifest for '" + key + "'; %w", err)
        }
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

func create_relative_symlink(relative_target, relative_link, full_link string) error {
    // Actually creating the link. We convert it to a relative path
    // within the registry so that the registry is relocatable.
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
        return fmt.Errorf("failed to create a symlink at '" + full_link + "'; %w", err)
    }
    return nil
}

func Transfer(source, registry, project, asset, version string) error {
    destination := filepath.Join(registry, project, asset, version)
    manifest := map[string]interface{}{}
    links := map[string]map[string]LinkMetadata{}
    manifest_cache := map[string]map[string]ManifestEntry{}
    summary_cache := map[string]bool{}

    // Loading the latest version's metadata into a deduplication index.
    last_dedup := map[string]LinkMetadata{}
    {
        asset_dir := filepath.Join(registry, project, asset)
        latest_path := filepath.Join(asset_dir, LatestFileName)

        _, err := os.Stat(latest_path)
        if err == nil {
            latest, err := ReadLatest(asset_dir)
            if err != nil {
                return fmt.Errorf("failed to identify the latest version; %w", err)
            }

            manifest, err := ReadManifest(filepath.Join(asset_dir, latest.Latest))
            if err != nil {
                return fmt.Errorf("failed to read the latest version's manifest; %w", err)
            }

            for k, v := range manifest {
                self := LinkMetadata{
                    Project: project,
                    Asset: asset,
                    Version: latest.Latest,
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
            return fmt.Errorf("failed to stat '" + latest_path + "; %w", err)
        }
    }

    err := filepath.WalkDir(source, func(path string, info fs.DirEntry, err error) error {
        if err != nil {
            return fmt.Errorf("failed to walk into '" + path + "'; %w", err)
        }
        if path == source {
            return nil
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

        // Symlinks to files inside the destination directory are preserved.
        restat, err := info.Info()
        if err != nil {
            return fmt.Errorf("failed to stat '" + path + "'; %w", err)
        }
        if restat.Mode() & os.ModeSymlink == os.ModeSymlink {
            target, err := os.Readlink(path)
            if err != nil {
                return fmt.Errorf("failed to read the symlink at '" + path + "'; %w", err)
            }

            inside, err := filepath.Rel(destination, target)
            if err == nil {
                obj, err := resolve_symlink(registry, project, asset, version, inside, manifest_cache, summary_cache)
                if err != nil {
                    return fmt.Errorf("failed to resolve the symlink at '" + path + "'; %w", err)
                }
                manifest[rel] = *obj

                err = create_relative_symlink(inside, rel, final)
                if err != nil {
                    return fmt.Errorf("failed to create a symlink for '" + rel + "'; %w", err)
                }

                subdir, base := filepath.Split(rel)
                sublinks, ok := links[subdir]
                if !ok {
                    sublinks = map[string]LinkMetadata{}
                    links[subdir] = sublinks
                }
                sublinks[base] = *(obj.Link)
                return nil
            }
        }

        insum, err := compute_checksum(path)
        if err != nil {
            return fmt.Errorf("failed to hash the source file; %w", err)
        }

        man_entry := ManifestEntry{
            Size: restat.Size(),
            Md5sum: insum,
        }

        // Seeing if we can create a link to the last version of the file with the same md5sum.
        last_entry, ok := last_dedup[strconv.FormatInt(man_entry.Size, 10) + "-" + man_entry.Md5sum]
        if ok {
            man_entry.Link = &last_entry
            manifest[rel] = man_entry
            err = create_relative_symlink(filepath.Join(last_entry.Project, last_entry.Asset, last_entry.Version, last_entry.Path), rel, final)
            if err != nil {
                return fmt.Errorf("failed to create a symlink for '" + rel + "'; %w", err)
            }
            return nil
        }

        err = copy_file(path, final)
        if err != nil {
            return fmt.Errorf("failed to copy file at '" + rel + "'; %w", err)
        }

        finalsum, err := compute_checksum(final)
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

    // Dumping the JSON metadata.
    manifest_str, err := json.MarshalIndent(&manifest, "", "    ")
    if err != nil {
        return fmt.Errorf("failed to convert the manifest to JSON; %w", err)
    }
    manifest_path := filepath.Join(destination, ManifestFileName)
    err = os.WriteFile(manifest_path, manifest_str, 0644)
    if err != nil {
        return fmt.Errorf("failed to save the manifest to '" + manifest_path + "'; %w", err)
    }

    for k, v := range links {
        link_str, err := json.MarshalIndent(&v, "", "    ")
        if err != nil {
            return fmt.Errorf("failed to convert link for '" + k + "' to JSON; %w", err)
        }
        link_path := filepath.Join(destination, k)
        err = os.WriteFile(link_path, link_str, 0644)
        if err != nil {
            return fmt.Errorf("failed to save the manifest to '" + link_path + "'; %w", err)
        }
    }

    return err
}
