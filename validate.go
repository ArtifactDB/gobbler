package main

import (
    "fmt"
    "context"
    "path/filepath"
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
