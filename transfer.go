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

func deduplicateLatestKey(size int64, md5sum string) string {
    return strconv.FormatInt(size, 10) + "-" + md5sum
}

func transferDirectory(source, registry, project, asset, version string, ctx context.Context, throttle *concurrencyThrottle, options transferDirectoryOptions) error {
    // Loading the latest version's metadata into a deduplication index.
    // There's no need to check for probational versions here as only non-probational versions ever make it into '..latest'.
    var last_dedup map[string]*linkMetadata
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

        last_dedup = make(map[string]*linkMetadata)
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
            last_dedup[deduplicateLatestKey(v.Size, v.Md5sum)] = self
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
        ctx,
        throttle,
        walkDirectoryOptions{
            Mode: WalkDirectoryTransfer,
            Consume: options.Consume,
            DeduplicateLatest: last_dedup,
            RestoreLinkParent: nil,
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
