package main

import (
    "testing"
    "os"
    "path/filepath"
    "fmt"
    "strings"
    "errors"
    "encoding/json"
    "context"
)

func TestTransferDirectorySimple(t *testing.T) {
    project := "pokemon"
    asset := "pikachu"
    version := "red"

    src, err := setupSourceForWalkDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    ctx := context.Background()
    conc := newConcurrencyThrottle(2)

    // Executing the transfer.
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
    if err != nil {
        t.Fatalf("failed to perform the transfer; %v", err)
    }

    // Checking a few manifest entries...
    destination := filepath.Join(reg, project, asset, version)
    man, err := readManifest(destination)
    if err != nil {
        t.Fatalf("failed to read the manifest; %v", err)
    }
    info, ok := man["evolution/up"]
    if !ok || int(info.Size) != len("raichu") || info.Link != nil {
        t.Errorf("unexpected manifest entry for 'evolution/up'; %v", info)
    }
    info, ok = man["moves/electric/thunder"]
    if !ok || int(info.Size) != len("110") || info.Link != nil {
        t.Errorf("unexpected manifest entry for 'moves/electric/thunder'; %v", info)
    }

    // Checking some of the actual files.
    err = verifyFileContents(filepath.Join(destination, "type"), "electric")
    if err != nil {
        t.Error(err)
    }
    err = verifyFileContents(filepath.Join(destination, "evolution", "down"), "pichu")
    if err != nil {
        t.Error(err)
    }
    err = verifyFileContents(filepath.Join(destination, "moves", "normal", "double_team"), "0")
    if err != nil {
        t.Error(err)
    }

    // Checking that there are no empty directories.
    for k, m := range man {
        if m.Md5sum == "" {
            t.Errorf("unexpected empty directory %q in manifest", k)
        }
    }

    // Fails with an expired context.
    expired, _ := context.WithTimeout(ctx, 0)
    err = transferDirectory(src, reg, project, asset, "blue", expired, &conc, transferDirectoryOptions{})
    if err == nil || !strings.Contains(err.Error(), "cancelled") {
        t.Errorf("expected a cancellation error; %v", err)
    }
}

func TestTransferDirectoryEmptyDirs(t *testing.T) {
    project := "pokemon"
    asset := "pikachu"
    version := "red"

    src, err := setupSourceForWalkDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()
    conc := newConcurrencyThrottle(2)

    // Adding some empty directories.
    err = os.Mkdir(filepath.Join(src, "rarity"), 0755)
    if err != nil {
        t.Fatal(err)
    }
    err = os.Mkdir(filepath.Join(src, "moves", "water"), 0755)
    if err != nil {
        t.Fatal(err)
    }
    err = os.MkdirAll(filepath.Join(src, "appearances", "yellow"), 0755) // the 'appearances' directory shouldn't show up in the manifest, it's not empty.
    if err != nil {
        t.Fatal(err)
    }

    err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
    if err != nil {
        t.Fatalf("failed to perform the transfer; %v", err)
    }

    destination := filepath.Join(reg, project, asset, version)
    man, err := readManifest(destination)
    if err != nil {
        t.Fatal(err)
    }

    for _, empty := range []string{"rarity", "moves/water", "appearances/yellow" } {
        if found, ok := man[empty]; !ok {
            t.Errorf("expected the %q empty directory to show up", empty)
        } else if found.Size != 0 || found.Md5sum != "" || found.Link != nil {
            t.Errorf("unexpected manifest entries for the %q empty directory", empty)
        } else if _, err := os.Stat(filepath.Join(destination, empty)); err != nil {
            t.Errorf("could not find the %q empty directory", empty)
        }
    }

    // Checking that no other directories were added here.
    for k, m := range man {
        if m.Md5sum == "" && k != "moves/water" && k != "rarity" && k != "appearances/yellow" {
            t.Errorf("unexpected empty directory %q in manifest", k)
        }
    }
}

func TestTransferDirectorySkipInternal(t *testing.T) {
    project := "pokemon"
    asset := "pikachu"
    version := "red"

    src, err := setupSourceForWalkDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()
    conc := newConcurrencyThrottle(2)

    // Injecting some internal files.
    err = os.WriteFile(filepath.Join(src, "..something"), []byte("something something"), 0644)
    if err != nil {
        t.Fatalf("failed to write internal file; %v", err)
    }

    err = os.Mkdir(filepath.Join(src, "..internal"), 0755)
    if err != nil {
        t.Fatalf("failed to make an internal directory; %v", err)
    }

    err = os.WriteFile(filepath.Join(src, "..internal", "credentials"), []byte("password"), 0644)
    if err != nil {
        t.Fatalf("failed to write file inside a internal directory; %v", err)
    }

    // Executing the transfer.
    err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
    if err != nil {
        t.Fatalf("failed to perform the transfer; %v", err)
    }

    destination := filepath.Join(reg, project, asset, version)
    if _, err := os.Stat(filepath.Join(destination, "..something")); err == nil || !errors.Is(err, os.ErrNotExist) {
        t.Error("internal files should not be transferred")
    }
    if _, err := os.Stat(filepath.Join(destination, "..internal", "credentials")); err == nil || !errors.Is(err, os.ErrNotExist) {
        t.Error("internal files should not be transferred")
    }
}

func TestTransferDirectorySkipHidden(t *testing.T) {
    project := "pokemon"
    asset := "pikachu"

    src, err := setupSourceForWalkDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()
    conc := newConcurrencyThrottle(2)

    // Injecting some hidden files.
    err = os.WriteFile(filepath.Join(src, ".DS_store"), []byte("some mac stuff"), 0644)
    if err != nil {
        t.Fatalf("failed to write hidden file; %v", err)
    }

    err = os.Mkdir(filepath.Join(src, ".git"), 0755)
    if err != nil {
        t.Fatalf("failed to make an hidden directory; %v", err)
    }

    err = os.WriteFile(filepath.Join(src, ".git", "something"), []byte("git"), 0644)
    if err != nil {
        t.Fatalf("failed to write file inside a hidden directory; %v", err)
    }

    t.Run("with dots", func(t *testing.T) {
        version := "red"
        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }
        destination := filepath.Join(reg, project, asset, version)
        if _, err := os.Stat(filepath.Join(destination, ".DS_store")); err != nil {
            t.Errorf("hidden files should be transferred; %v", err)
        }
        if _, err := os.Stat(filepath.Join(destination, ".git", "something")); err != nil {
            t.Errorf("hidden files should be transferred; %v", err)
        }
    })

    t.Run("ignore dots", func(t *testing.T) {
        version := "blue"
        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{ IgnoreDot: true })
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }
        destination := filepath.Join(reg, project, asset, version)
        if _, err := os.Stat(filepath.Join(destination, "..something")); err == nil || !errors.Is(err, os.ErrNotExist) {
            t.Error("hidden files should not be transferred")
        }
        if _, err := os.Stat(filepath.Join(destination, "..hidden", "credentials")); err == nil || !errors.Is(err, os.ErrNotExist) {
            t.Error("hidden files should not be transferred")
        }
    })
}

func TestTransferDirectoryConsume(t *testing.T) {
    project := "pokemon"
    asset := "pikachu" 
    version := "yellow"

    ctx := context.Background()
    conc := newConcurrencyThrottle(2)

    // Executing the transfer; by default, nothing is moved, until Consume=true.
    t.Run("no consume", func(t *testing.T) {
        src, err := setupSourceForWalkDirectoryTest()
        if err != nil {
            t.Fatalf("failed to set up test directories; %v", err)
        }

        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the registry; %v", err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }

        _, err = os.Stat(filepath.Join(src, "evolution", "up"))
        if err != nil {
            t.Errorf("source files should not have been moved; %v", err)
        }

        _, err = os.Stat(filepath.Join(src, "moves", "normal", "quick_attack"))
        if err != nil {
            t.Errorf("source files should not have been moved; %v", err)
        }
    })

    // Until Consume=true.
    t.Run("with consume", func(t *testing.T) {
        src, err := setupSourceForWalkDirectoryTest()
        if err != nil {
            t.Fatalf("failed to set up test directories; %v", err)
        }

        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the registry; %v", err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{ Consume: true })
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }

        destination := filepath.Join(reg, project, asset, version)
        if _, err := os.Stat(filepath.Join(destination, "evolution", "up")); err != nil {
            t.Errorf("failed to successfully transfer file; %v", err)
        }

        _, err = os.Stat(filepath.Join(src, "evolution", "up"))
        if err == nil || !errors.Is(err, os.ErrNotExist) {
            t.Errorf("source files should have been moved; %v", err)
        }

        _, err = os.Stat(filepath.Join(src, "moves", "normal", "quick_attack"))
        if err == nil || !errors.Is(err, os.ErrNotExist) {
            t.Errorf("source files should have been moved; %v", err)
        }
    })

    t.Run("local links", func(t *testing.T) {
        src, err := setupSourceForWalkDirectoryTest()
        if err != nil {
            t.Fatalf("failed to set up test directories; %v", err)
        }

        // Adding a local link to check that they aren't invalidated by moves. We add 
        // some that sort before and after 'double_team' to check that the order doesn't matter.
        err = os.Symlink("double_team", filepath.Join(src, "moves", "normal", "charm"))
        if err != nil {
            t.Fatal(err)
        }
        err = os.Symlink("double_team", filepath.Join(src, "moves", "normal", "growl"))
        if err != nil {
            t.Fatal(err)
        }

        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the registry; %v", err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{ Consume: true })
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }

        destination := filepath.Join(reg, project, asset, version)
        target, err := os.Readlink(filepath.Join(destination, "moves", "normal", "charm"))
        if err != nil || target != "double_team" {
            t.Errorf("expected 'charm' to link to 'double_team'; %v", err)
        }
        err = verifyFileContents(filepath.Join(destination, "moves", "normal", "charm"), "0")
        if err != nil {
            t.Error(err)
        }

        target, err = os.Readlink(filepath.Join(destination, "moves", "normal", "growl"))
        if err != nil || target != "double_team" {
            t.Errorf("expected 'growl' to link to 'double_team'; %v", err)
        }
        err = verifyFileContents(filepath.Join(destination, "moves", "normal", "growl"), "0")
        if err != nil {
            t.Error(err)
        }
    })
}

/**********************************************
 **********************************************/

func extractSymlinkTarget(path string) (string, error) {
    finfo, err := os.Lstat(path)
    if err != nil {
        return "", fmt.Errorf("failed to stat %q; %w", path, err)
    }
    if finfo.Mode() & os.ModeSymlink != os.ModeSymlink {
        return "", fmt.Errorf("expected %q to be a symlink; %w", path, err)
    }

    target, err := os.Readlink(path)
    if err != nil {
        return "", fmt.Errorf("failed to read the symlink from %q; %w", path, err)
    }

    return target, nil
}

func verifySymlink(
    manifest map[string]manifestEntry,
    version_dir,
    path,
    contents,
    target_project,
    target_asset,
    target_version,
    target_path string,
    is_local bool,
    has_ancestor bool,
) error {
    info, ok := manifest[path]
    if !ok || 
        int(info.Size) != len(contents) || 
        info.Link == nil || 
        info.Link.Project != target_project || 
        info.Link.Asset != target_asset || 
        info.Link.Version != target_version || 
        info.Link.Path != target_path ||
        has_ancestor != (info.Link.Ancestor != nil) {
        return fmt.Errorf("unexpected manifest entry for %q", path)
    }

    full := filepath.Join(version_dir, path)
    err := verifyFileContents(full, contents)
    if err != nil {
        return err
    }

    target, err := extractSymlinkTarget(full)
    if err != nil {
        return err
    }

    okay := true
    if filepath.IsAbs(target) { // should always be relative to enable painless relocation of the registry.
        okay = false
    } else {
        candidate := filepath.Clean(filepath.Join(filepath.Dir(full), target))
        registry := filepath.Dir(filepath.Dir(filepath.Dir(version_dir)))

        var expected_dest string
        if has_ancestor {
            expected_dest = filepath.Join(registry, info.Link.Ancestor.Project, info.Link.Ancestor.Asset, info.Link.Ancestor.Version, info.Link.Ancestor.Path)
        } else {
            expected_dest = filepath.Join(registry, target_project, target_asset, target_version, target_path)
        }
        if expected_dest != candidate {
            okay = false
        }

        if is_local {
            rellocal, err := filepath.Rel(filepath.Dir(version_dir), expected_dest)
            if err != nil || !filepath.IsLocal(rellocal) {
                okay = false
            }
        }
    }
    if !okay {
        return fmt.Errorf("unexpected symlink format for %q (got %q)", path, target)
    }

    dir, base := filepath.Split(path)
    linkmeta_path := filepath.Join(version_dir, dir, linksFileName)
    linkmeta_raw, err := os.ReadFile(linkmeta_path)
    if err != nil {
        return fmt.Errorf("failed to read the link metadata; %w", err)
    }

    var linkmeta map[string]linkMetadata
    err = json.Unmarshal(linkmeta_raw, &linkmeta)
    if err != nil {
        return fmt.Errorf("failed to parse the link metadata; %w", err)
    }

    found, ok := linkmeta[base]
    if !ok {
        return fmt.Errorf("failed to find %q in the link metadata of %q", base, dir)
    }

    if found.Project != target_project || 
        found.Asset != target_asset || 
        found.Version != target_version || 
        found.Path != target_path ||
        has_ancestor != (found.Ancestor != nil) {
        return fmt.Errorf("unexpected link metadata entry for %q", path)
    }

    return nil
}

func verifyRegistrySymlink(
    manifest map[string]manifestEntry,
    version_dir,
    path,
    contents,
    target_project,
    target_asset,
    target_version,
    target_path string,
    has_ancestor bool,
) error {
    return verifySymlink(manifest, version_dir, path, contents, target_project, target_asset, target_version, target_path, false, has_ancestor)
}

func verifyLocalSymlink(
    manifest map[string]manifestEntry,
    version_dir,
    path,
    contents,
    target_project,
    target_asset,
    target_version,
    target_path string,
    has_ancestor bool,
) error {
    return verifySymlink(manifest, version_dir, path, contents, target_project, target_asset, target_version, target_path, true, has_ancestor)
}

func verifyNotSymlink(manifest map[string]manifestEntry, version_dir, path, contents string) error {
    info, ok := manifest[path]
    if !ok || int(info.Size) != len(contents) || info.Link != nil {
        return fmt.Errorf("unexpected manifest entry for %q", path)
    }

    full := filepath.Join(version_dir, path)
    err := verifyFileContents(full, contents)
    if err != nil {
        return err
    }

    return nil
}

func verifyAncestralSymlink(
    manifest map[string]manifestEntry, 
    version_dir, 
    path, 
    registry,
    ancestor_project, 
    ancestor_asset, 
    ancestor_version, 
    ancestor_path string,
) error {
    info, ok := manifest[path]
    if !ok || 
        info.Link == nil || 
        info.Link.Ancestor == nil ||
        info.Link.Ancestor.Project != ancestor_project || 
        info.Link.Ancestor.Asset != ancestor_asset || 
        info.Link.Ancestor.Version != ancestor_version || 
        info.Link.Ancestor.Path != ancestor_path {
        return fmt.Errorf("unexpected manifest entry for %q in %q", path, version_dir)
    }

    host, err := os.Stat(filepath.Join(version_dir, path))
    if err != nil {
        return fmt.Errorf("failed to stat the link target; %w", err)
    }

    ancestor, err := os.Stat(filepath.Join(registry, ancestor_project, ancestor_asset, ancestor_version, ancestor_path))
    if err != nil {
        return fmt.Errorf("failed to stat the ancestor; %w", err)
    }

    if !os.SameFile(host, ancestor) {
        return errors.New("link does not point to the same file as the ancestor")
    }

    return nil
}

/**********************************************
 **********************************************/

func TestTransferDirectoryDeduplication(t *testing.T) {
    project := "pokemon"
    asset := "pikachu"
    version := "red"

    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()
    conc := newConcurrencyThrottle(2)

    src, err := setupSourceForWalkDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Executing the first transfer.
    {
        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }

        err = os.WriteFile(filepath.Join(reg, project, asset, latestFileName), []byte("{ \"version\": \"" + version + "\" }"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }
    }

    // Modifying the directory and executing the second transfer.
    {
        err = os.Remove(filepath.Join(src, "moves", "normal", "double_team"))
        if err != nil {
            t.Fatalf("failed to delete 'moves/normal/double_team'")
        }

        err = os.WriteFile(filepath.Join(src, "moves", "electric", "thunder_shock"), []byte("some_different_value"), 0644)
        if err != nil {
            t.Fatalf("failed to modify 'moves/electric/thunder_shock'")
        }

        err = os.Rename(filepath.Join(src, "evolution", "up"), filepath.Join(src, "evolution", "next"))
        if err != nil {
            t.Fatalf("failed to move 'evolution/up'")
        }

        err = os.Mkdir(filepath.Join(src, "moves", "steel"), 0755)
        if err != nil {
            t.Fatalf("failed to create 'moves/steel'")
        }
        err = os.WriteFile(filepath.Join(src, "moves", "steel", "iron_tail"), []byte("100"), 0644)
        if err != nil {
            t.Fatalf("failed to write 'moves/steel/iron_tail'")
        }

        new_version := "blue"
        err = transferDirectory(src, reg, project, asset, new_version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }

        err = os.WriteFile(filepath.Join(reg, project, asset, latestFileName), []byte("{ \"version\": \"" + new_version + "\" }"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }

        destination := filepath.Join(reg, project, asset, new_version)
        man, err := readManifest(destination)
        if err != nil {
            t.Fatalf("failed to read the manifest; %v", err)
        }

        // Different file name.
        err = verifyRegistrySymlink(man, destination, "evolution/next", "raichu", project, asset, version, "evolution/up", false)
        if err != nil {
            t.Error(err)
        }

        // Same file name.
        err = verifyRegistrySymlink(man, destination, "moves/electric/thunder", "110", project, asset, version, "moves/electric/thunder", false)
        if err != nil {
            t.Error(err)
        }

        // Modified file.
        err = verifyNotSymlink(man, destination, "moves/electric/thunder_shock", "some_different_value")
        if err != nil {
            t.Error(err)
        }

        // New file.
        err = verifyNotSymlink(man, destination, "moves/steel/iron_tail", "100")
        if err != nil {
            t.Error(err)
        }
    }

    // Modifying the directory and executing the transfer AGAIN to check for correct formulation of ancestral links.
    {
        err = os.Rename(filepath.Join(src, "evolution", "next"), filepath.Join(src, "evolution", "final"))
        if err != nil {
            t.Fatalf("failed to move 'evolution/next'; %v", err)
        }

        err = os.WriteFile(filepath.Join(src, "moves", "electric", "thunder_shock"), []byte("9999"), 0644)
        if err != nil {
            t.Fatalf("failed to modify 'moves/electric/thunder_shock'")
        }

        err = os.WriteFile(filepath.Join(src, "moves", "normal", "feint"), []byte("30"), 0644)
        if err != nil {
            t.Fatalf("failed to write 'moves/normal/feint'; %v", err)
        }

        new_version := "green"
        err = transferDirectory(src, reg, project, asset, new_version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }

        err = os.WriteFile(filepath.Join(reg, project, asset, latestFileName), []byte("{ \"version\": \"" + new_version + "\" }"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }

        destination := filepath.Join(reg, project, asset, new_version)
        man, err := readManifest(destination)
        if err != nil {
            t.Fatalf("failed to read the manifest; %v", err)
        }

        err = verifyRegistrySymlink(man, destination, "evolution/final", "raichu", project, asset, "blue", "evolution/next", true)
        if err != nil {
            t.Error(err)
        }
        err = verifyAncestralSymlink(man, destination, "evolution/final", reg, project, asset, "red", "evolution/up") 
        if err != nil {
            t.Error(err)
        }

        err = verifyRegistrySymlink(man, destination, "moves/electric/thunderbolt", "90", project, asset, "blue", "moves/electric/thunderbolt", true)
        if err != nil {
            t.Error(err)
        }
        err = verifyAncestralSymlink(man, destination, "moves/electric/thunderbolt", reg, project, asset, "red", "moves/electric/thunderbolt") 
        if err != nil {
            t.Error(err)
        }

        err = verifyNotSymlink(man, destination, "moves/electric/thunder_shock", "9999")
        if err != nil {
            t.Error(err)
        }

        err = verifyNotSymlink(man, destination, "moves/normal/feint", "30")
        if err != nil {
            t.Error(err)
        }

        err = verifyRegistrySymlink(man, destination, "moves/steel/iron_tail", "100", project, asset, "blue", "moves/steel/iron_tail", false)
        if err != nil {
            t.Error(err)
        }
    }

    // Executing the transfer AGAIN to check that ancestral links of the older version are themselves respected.
    {
        new_version := "yellow"
        err = transferDirectory(src, reg, project, asset, new_version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }

        err = os.WriteFile(filepath.Join(reg, project, asset, latestFileName), []byte("{ \"version\": \"" + new_version + "\" }"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }

        destination := filepath.Join(reg, project, asset, new_version)
        man, err := readManifest(destination)
        if err != nil {
            t.Fatalf("failed to read the manifest; %v", err)
        }

        err = verifyRegistrySymlink(man, destination, "evolution/final", "raichu", project, asset, "green", "evolution/final", true)
        if err != nil {
            t.Error(err)
        }
        err = verifyAncestralSymlink(man, destination, "evolution/final", reg, project, asset, "red", "evolution/up") 
        if err != nil {
            t.Error(err)
        }

        err = verifyRegistrySymlink(man, destination, "moves/electric/thunder_shock", "9999", project, asset, "green", "moves/electric/thunder_shock", false)
        if err != nil {
            t.Error(err)
        }

        // We can also form new ancestral links.
        err = verifyRegistrySymlink(man, destination, "moves/steel/iron_tail", "100", project, asset, "green", "moves/steel/iron_tail", true)
        if err != nil {
            t.Error(err)
        }
        err = verifyAncestralSymlink(man, destination, "moves/steel/iron_tail", reg, project, asset, "blue", "moves/steel/iron_tail")
        if err != nil {
            t.Error(err)
        }
    }
}

/**********************************************
 **********************************************/

func TestTransferDirectoryRegistryLinks(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()
    conc := newConcurrencyThrottle(2)

    src, err := setupSourceForWalkDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Mocking up a directory structure and executing a series of transfers to create appropriate links. 
    {
        project := "pokemon"
        asset := "pikachu"

        err = transferDirectory(src, reg, project, asset, "red", ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }
        err = os.WriteFile(filepath.Join(reg, project, asset, "red", summaryFileName), []byte("{}"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }
        err = os.WriteFile(filepath.Join(reg, project, asset, latestFileName), []byte("{ \"version\": \"red\" }"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }

        err = transferDirectory(src, reg, project, asset, "blue", ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }
        err = os.WriteFile(filepath.Join(reg, project, asset, "blue", summaryFileName), []byte("{}"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }
        err = os.WriteFile(filepath.Join(reg, project, asset, latestFileName), []byte("{ \"version\": \"blue\" }"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }

        err = transferDirectory(src, reg, project, asset, "green", ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }
        err = os.WriteFile(filepath.Join(reg, project, asset, "green", summaryFileName), []byte("{}"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }
        err = os.WriteFile(filepath.Join(reg, project, asset, latestFileName), []byte("{ \"version\": \"green\" }"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }
    }

    // Making a new source with links to the registry.
    {
        src, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create a new source directory; %v", err)
        }

        err = os.Mkdir(filepath.Join(src, "types"), 0755)
        if err != nil {
            t.Fatalf("failed to create an 'types' subdirectory; %v", err)
        }
        err = os.Symlink(filepath.Join(reg, "pokemon", "pikachu", "red", "type"), filepath.Join(src, "types", "first"))
        if err != nil {
            t.Fatalf("failed to create a symlink for 'types/first'; %v", err)
        }
        err = os.WriteFile(filepath.Join(src, "types", "second"), []byte("steel"), 0644)
        if err != nil {
            t.Fatalf("failed to write file for 'types/second'; %v", err)
        }

        err = os.MkdirAll(filepath.Join(src, "moves", "electric"), 0755)
        if err != nil {
            t.Fatalf("failed to create an 'moves/electric' subdirectory; %v", err)
        }
        err = os.Symlink(filepath.Join(reg, "pokemon", "pikachu", "blue", "moves", "electric", "thunderbolt"), filepath.Join(src, "moves", "electric", "THUNDERBOLT"))
        if err != nil {
            t.Fatalf("failed to create a symlink for 'moves/electric/THUNDERBOLT'; %v", err)
        }

        err = os.Symlink(filepath.Join(reg, "pokemon", "pikachu", "green", "evolution", "down"), filepath.Join(src, "best_friend"))
        if err != nil {
            t.Fatalf("failed to create a symlink for 'best_friend'; %v", err)
        }

        project := "more_pokemon"
        asset := "magneton"
        err = transferDirectory(src, reg, project, asset, "kanto", ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }

        destination := filepath.Join(reg, project, asset, "kanto")
        man, err := readManifest(destination)
        if err != nil {
            t.Fatalf("failed to read the manifest; %v", err)
        }

        err = verifyRegistrySymlink(man, destination, "types/first", "electric", "pokemon", "pikachu", "red", "type", false)
        if err != nil {
            t.Error(err)
        }

        err = verifyRegistrySymlink(man, destination, "moves/electric/THUNDERBOLT", "90", "pokemon", "pikachu", "blue", "moves/electric/thunderbolt", true)
        if err != nil {
            t.Error(err)
        }
        err = verifyAncestralSymlink(man, destination, "moves/electric/THUNDERBOLT", reg, "pokemon", "pikachu", "red", "moves/electric/thunderbolt")
        if err != nil {
            t.Error(err)
        }

        err = verifyRegistrySymlink(man, destination, "best_friend", "pichu", "pokemon", "pikachu", "green", "evolution/down", true)
        if err != nil {
            t.Error(err)
        }
        err = verifyAncestralSymlink(man, destination, "best_friend", reg, "pokemon", "pikachu", "red", "evolution/down")
        if err != nil {
            t.Error(err)
        }

        err = verifyNotSymlink(man, destination, "types/second", "steel")
        if err != nil {
            t.Error(err)
        }
    }
}

func TestTransferDirectoryRegistryLinkFailures(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()
    conc := newConcurrencyThrottle(2)

    t.Run("loose registry files", func(t *testing.T) {
        src, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        other_path := filepath.Join(reg, "FOOBAR")
        if err := os.WriteFile(other_path, []byte("gotta catch em all"), 0644); err != nil {
            t.Fatalf("failed to write a random temporary file; %v", err)
        }

        err = os.Symlink(other_path, filepath.Join(src, "asdasd"))
        if err != nil {
            t.Fatalf("failed to create a test link to a random file")
        }

        project := "POKEMON"
        asset := "PIKAPIKA"
        version := "GOLD"
        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "outside of a project asset version directory") {
            t.Errorf("expected a failure when linking to loose files; %v", err)
        }
    })

    t.Run("currently transferring", func(t *testing.T) {
        src, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        project := "POKEMON"
        asset := "PIKAPIKA"
        version := "SILVER"

        // Creating a link to target the same version that's currently being
        // created. In practice, this should not work as the initial Stat() of
        // the link requires the file to already be in the registry. Files for
        // the currently-created version are only copied to the registry in the
        // second pass after the Stat(), so the file could only exist if the
        // version is already present, and this should be prohibited by the
        // upload endpoint... nonetheless, defence in depth, so here we are.
        dest := filepath.Join(reg, project, asset, version)
        err = os.Mkdir(dest, 0755)
        if err != nil {
            t.Fatal(err)
        }
        err = os.WriteFile(filepath.Join(dest, "aaa"), []byte("123123123123"), 0644)
        if err != nil {
            t.Fatal(err)
        }
        err = os.Symlink(filepath.Join(dest, "aaa"), filepath.Join(src, "zzz")) 
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "currently-transferring") {
            t.Errorf("expected a failure when linking to the currently-transferring version; %v", err)
        }
    })

    t.Run("probational versions", func(t *testing.T) {
        src, err := setupSourceForWalkDirectoryTest()
        if err != nil {
            t.Fatalf("failed to set up test directories; %v", err)
        }

        project := "POKEMON"
        asset := "GYARADOS"
        version := "yellow"
        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }
        err = os.WriteFile(filepath.Join(reg, project, asset, version, summaryFileName), []byte(`{ "on_probation": true }`), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }

        err = os.Symlink(filepath.Join(reg, project, asset, version, "type"), filepath.Join(src, "asdasd"))
        if err != nil {
            t.Fatalf("failed to create a test link to a registry file")
        }

        version = "crystal"
        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "probational") {
            t.Errorf("expected a failure when linking to a probational version; %v", err)
        }
    })

    t.Run("internal files", func(t *testing.T) {
        src, err := setupSourceForWalkDirectoryTest()
        if err != nil {
            t.Fatalf("failed to set up test directories; %v", err)
        }

        project := "POKEMON"
        asset := "MACHOMP"
        version := "red"
        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }
        err = os.WriteFile(filepath.Join(reg, project, asset, version, summaryFileName), []byte(`{}`), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }

        err = os.Symlink(filepath.Join(reg, project, asset, version, summaryFileName), filepath.Join(src, "asdasd"))
        if err != nil {
            t.Fatalf("failed to create a test link to a registry file")
        }

        version = "blue"
        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "internal '..' files") {
            t.Errorf("expected a failure when linking to internal files; %v", err)
        }
    })

    t.Run("directory links", func(t *testing.T) {
        src, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        mock, err := os.MkdirTemp(reg, "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory as a link target; %v", err)
        }

        if err := os.Symlink(mock, filepath.Join(src, "WHEE")); err != nil {
            t.Fatalf("failed to make a symlink to the mock directory; %v", err)
        }

        project := "POKEMON"
        asset := "VILEPLUME"
        version := "green"
        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "is a directory") {
            t.Errorf("expected a failure when a symbolic link to a directory is present; %v", err)
        }
    })
}

/**********************************************
 **********************************************/

func TestTransferDirectoryLocalLinks(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()
    conc := newConcurrencyThrottle(2)

    src, err := setupSourceForWalkDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    err = os.Symlink(filepath.Join(src, "type"), filepath.Join(src, "type2"))
    if err != nil {
        t.Fatalf("failed to create a symlink for 'types2'; %v", err)
    }

    err = os.Symlink(filepath.Join(src, "type2"), filepath.Join(src, "evolution", "foo")) // symlink to another symlink.
    if err != nil {
        t.Fatalf("failed to create a symlink for 'evolution/foo'; %v", err)
    }

    err = os.Symlink(filepath.Join("..", "type2"), filepath.Join(src, "evolution", "bar")) // same, but as a relative link.
    if err != nil {
        t.Fatalf("failed to create a symlink for 'evolution/bar'; %v", err)
    }

    err = os.Symlink(filepath.Join("evolution", "up"), filepath.Join(src, "WHEE")) // relative symlink to subdirectory.
    if err != nil {
        t.Fatalf("failed to create a symlink for 'WHEE'; %v", err)
    }

    project := "POKEMON"
    asset := "PIKAPIKA"
    version := "GOLD"

    err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
    if err != nil {
        t.Fatalf("failed to perform the transfer; %v", err)
    }

    destination := filepath.Join(reg, project, asset, version)
    man, err := readManifest(destination)
    if err != nil {
        t.Fatalf("failed to read the manifest; %v", err)
    }

    err = verifyLocalSymlink(man, destination, "type2", "electric", project, asset, version, "type", false)
    if err != nil {
        t.Error(err)
    }

    err = verifyLocalSymlink(man, destination, "evolution/foo", "electric", project, asset, version, "type2", true)
    if err != nil {
        t.Error(err)
    }
    err = verifyAncestralSymlink(man, destination, "evolution/foo", reg, project, asset, version, "type")
    if err != nil {
        t.Error(err)
    }

    err = verifyLocalSymlink(man, destination, "evolution/bar", "electric", project, asset, version, "type2", true)
    if err != nil {
        t.Error(err)
    }
    err = verifyAncestralSymlink(man, destination, "evolution/bar", reg, project, asset, version, "type")
    if err != nil {
        t.Error(err)
    }

    err = verifyLocalSymlink(man, destination, "WHEE", "raichu", project, asset, version, "evolution/up", false)
    if err != nil {
        t.Error(err)
    }
}

func TestTransferDirectoryLocalLinkFailures(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()
    conc := newConcurrencyThrottle(2)

    t.Run("cyclic links", func(t* testing.T) {
        src, err := setupSourceForWalkDirectoryTest()
        if err != nil {
            t.Fatalf("failed to set up test directories; %v", err)
        }

        err = os.Symlink(filepath.Join(src, "foo"), filepath.Join(src, "bar"))
        if err != nil {
            t.Fatalf("failed to create a symlink for 'bar'; %v", err)
        }

        err = os.Symlink(filepath.Join(src, "bar"), filepath.Join(src, "evolution/whee")) 
        if err != nil {
            t.Fatalf("failed to create a symlink for 'evolution/whee'; %v", err)
        }

        err = os.Symlink(filepath.Join(src, "evolution/whee"), filepath.Join(src, "foo")) 
        if err != nil {
            t.Fatalf("failed to create a symlink for 'foo'; %v", err)
        }

        project := "POKEMON"
        asset := "PIKAPIKA"
        version := "GOLD"

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "too many levels") {
            t.Errorf("failed to detect cyclic local links; %v", err)
        }
    })

    t.Run("directory links", func(t *testing.T) {
        src, err := setupSourceForWalkDirectoryTest()
        if err != nil {
            t.Fatalf("failed to set up test directories; %v", err)
        }

        err = os.Symlink(filepath.Join(src, "evolution"), filepath.Join(src, "FOOBAR")) 
        if err != nil {
            t.Fatalf("failed to create a symlink for 'FOOBAR'; %v", err)
        }

        project := "POKEMON"
        asset := "PIKAPIKA"
        version := "SILVER"

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "is a directory") {
            t.Errorf("failed to detect links to a directory; %v", err)
        }
    })
}

/**********************************************
 **********************************************/

func TestTransferDirectoryExternalLinks(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()
    conc := newConcurrencyThrottle(2)

    src, err := setupSourceForWalkDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    other, err := os.CreateTemp("", "")
    if err != nil {
        t.Fatalf("failed to create a random temporary file; %v", err)
    }
    message := "gotta catch em all"
    if _, err := other.WriteString(message); err != nil {
        t.Fatalf("failed to write a random temporary file; %v", err)
    }
    other_name := other.Name()
    if err := other.Close(); err != nil {
        t.Fatalf("failed to close a random temporary file; %v", err)
    }

    err = os.Symlink(other_name, filepath.Join(src, "asdasd"))
    if err != nil {
        t.Fatalf("failed to create a test link to a random file")
    }

    // Without whitelisting, a copy is made.
    t.Run("no whitelist", func(t *testing.T) {
        project := "POKEMON"
        asset := "SQUIRTLE"
        version := "SILVER"
        err := transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err == nil && !strings.Contains(err.Error(), "not allowed") {
            t.Error("expected a failure when symlinking to a non-whitelisted directory")
        }
    })

    // Now with whitelisting.
    t.Run("whitelist", func(t *testing.T) {
        project := "POKEMON"
        asset := "PIKAPIKA"
        version := "SILVER"
        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{ LinkWhitelist: []string{ filepath.Dir(other_name) } })
        if err != nil {
            t.Fatal(err)
        }

        dest := filepath.Join(reg, project, asset, version)
        target, err := os.Readlink(filepath.Join(dest, "asdasd"))
        if err != nil {
            t.Fatal(err)
        }
        if target != other_name {
            t.Error("unexpected target of the whitelisted symlink")
        }

        man, err := readManifest(dest)
        if err != nil {
            t.Fatal(err)
        }

        contents, found := man["asdasd"]
        if !found || contents.Link != nil || contents.Size != int64(len(message)) {
            t.Error("unexpected manifest entry for whitelisted symlink")
        }
    })
}

