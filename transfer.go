package main

import (
    "path/filepath"
    "fmt"
    "os"
    "errors"
    "context"
    "strconv"
)

type transferDirectoryOptions struct {
    Consume bool
    IgnoreDot bool
    LinkWhitelist []string
}

func transferDirectory(source, registry, project, asset, version string, ctx context.Context, throttle *concurrencyThrottle, options transferDirectoryOptions) error {
    // Loading the latest version's metadata into a deduplication index.
    // There's no need to check for probational versions here as only non-probational versions ever make it into '..latest'.
    last_dedup := map[string]*linkMetadata{}
    asset_dir := filepath.Join(registry, project, asset)
    latest_path := filepath.Join(asset_dir, latestFileName)

    _, err := os.Stat(latest_path)
    if err == nil {
        latest, err := readLatest(asset_dir)
        if err != nil {
            return fmt.Errorf("failed to identify the latest version; %w", err)
        }

        manifest, err := readManifest(filepath.Join(asset_dir, latest.Version))
        if err != nil {
            return fmt.Errorf("failed to read the latest version's manifest; %w", err)
        }

        for k, v := range manifest {
            self := &linkMetadata{
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
        return fmt.Errorf("failed to stat '" + latest_path + "; %w", err)
    }

    manifest, err := walkDirectory(
        source,
        registry,
        project,
        asset,
        version,
        createSymlink,
        func(from, to string) string {
            return to
        },
        func(path string, man manifestEntry) *linkMetadata {
            // Seeing if we can create a link to the last version's file with the same md5sum.
            last_entry, ok := last_dedup[strconv.FormatInt(man.Size, 10) + "-" + man.Md5sum]
            if ok {
                return last_entry;
            } else {
                return nil
            }
        },
        ctx,
        throttle,
        walkDirectoryOptions{
            Transfer: true,
            Consume: options.Consume,
            IgnoreDot: options.IgnoreDot,
            LinkWhitelist: options.LinkWhitelist,
        },
    )
    if err != nil {
        return err
    }

    destination := filepath.Join(asset_dir, version)
    manifest_path := filepath.Join(destination, manifestFileName)
    err = dumpJson(manifest_path, &manifest)
    if err != nil {
        return fmt.Errorf("failed to save manifest for %q; %w", destination, err)
    }

    _, err = recreateLinkFiles(destination, manifest)
    if err != nil {
        return fmt.Errorf("failed to create linkfiles; %w", err)
    }

    return nil
}
