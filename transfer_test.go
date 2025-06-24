package main

import (
    "testing"
    "os"
    "path/filepath"
    "fmt"
    "strings"
    "errors"
    "encoding/json"
    "io/fs"
    "sort"
    "context"
)

func setupSourceForTransferDirectoryTest() (string, error) {
    dir, err := os.MkdirTemp("", "")
    if err != nil {
        return "", fmt.Errorf("failed to create the temporary directory; %w", err)
    }

    err = os.WriteFile(filepath.Join(dir, "type"), []byte("electric"), 0644)
    if err != nil {
        return "", err
    }

    err = os.Mkdir(filepath.Join(dir, "evolution"), 0755)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(dir, "evolution", "up"), []byte("raichu"), 0644)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(dir, "evolution", "down"), []byte("pichu"), 0644)
    if err != nil {
        return "", err
    }

    err = os.Mkdir(filepath.Join(dir, "moves"), 0755)
    if err != nil {
        return "", err
    }
    err = os.Mkdir(filepath.Join(dir, "moves", "electric"), 0755)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(dir, "moves", "electric", "thunder_shock"), []byte("40"), 0644)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(dir, "moves", "electric", "thunderbolt"), []byte("90"), 0644)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(dir, "moves", "electric", "thunder"), []byte("110"), 0644)
    if err != nil {
        return "", err
    }

    err = os.Mkdir(filepath.Join(dir, "moves", "normal"), 0755)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(dir, "moves", "normal", "quick_attack"), []byte("40"), 0644)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(dir, "moves", "normal", "double_team"), []byte("0"), 0644)
    if err != nil {
        return "", err
    }

    return dir, nil
}

func stripDoubleDotFiles(dir string) error {
    return filepath.WalkDir(dir, func(path string, info fs.DirEntry, err error) error {
        if !strings.HasPrefix(filepath.Base(path), "..") {
            return nil
        }
        if info.IsDir() {
            return nil
        }
        return os.Remove(path)
    })
}

type customDirEntry struct {
    LinkTarget string 
    Contents string
}

func loadDirectoryContents(dir string) (map[string]customDirEntry, error) {
    contents := map[string]customDirEntry{}

    err := filepath.WalkDir(dir, func(path string, info fs.DirEntry, err error) error {
        if info.IsDir() {
            return nil
        }

        restat, err := info.Info()
        if err != nil {
            return fmt.Errorf("failed to stat '" + path + "'; %w", err)
        }

        rel, err := filepath.Rel(dir, path)
        if err != nil {
            return fmt.Errorf("failed to create a relative path from %q; %w", path, err)
        }

        // Symlinks to files inside the registry are preserved.
        if restat.Mode() & os.ModeSymlink == os.ModeSymlink {
            target, err := os.Readlink(path)
            if err != nil {
                return fmt.Errorf("failed to read symlink for %q; %w", path, err)
            }
            contents[rel] = customDirEntry{ LinkTarget: target }
            return nil
        }

        fcontents, err := os.ReadFile(path)
        if err != nil {
            return fmt.Errorf("failed to read %q; %w", path, err)
        }
        contents[rel] = customDirEntry{ Contents: string(fcontents) }
        return nil
    })

    return contents, err
}

func compareDirectoryContents(ref map[string]customDirEntry, current map[string]customDirEntry) error {
    if len(ref) != len(current) {
        r_names := []string{}
        for k, _ := range ref {
            r_names = append(r_names, k)
        }
        c_names := []string{}
        for r, _ := range current {
            c_names = append(c_names, r)
        }
        sort.Strings(c_names)
        sort.Strings(r_names)
        return fmt.Errorf("mismatch in the number of files after reindexing; %v versus %v", r_names, c_names)
    }

    for k, entry := range ref {
        re_entry, found := current[k]
        if !found {
            return fmt.Errorf("failed to find %q after reindexing", k)
        }
        if re_entry.Contents != entry.Contents {
            return fmt.Errorf("mismatch in contents of %q after reindexing", k)
        } else if re_entry.LinkTarget != entry.LinkTarget {
            return fmt.Errorf("mismatch in link targets of %q after reindexing", k)
        }
    }

    return nil
}

/**********************************************
 **********************************************/

func TestTransferDirectorySimple(t *testing.T) {
    project := "pokemon"
    asset := "pikachu"
    version := "red"

    src, err := setupSourceForTransferDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    ctx := context.Background()

    // Executing the transfer.
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
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

    // Fails with an expired context.
    expired, _ := context.WithTimeout(ctx, 0)
    err = transferDirectory(src, reg, project, asset, "blue", expired, transferDirectoryOptions{})
    if err == nil || !strings.Contains(err.Error(), "cancelled") {
        t.Errorf("expected a cancellation error; %v", err)
    }
}

func TestTransferDirectorySkipInternal(t *testing.T) {
    project := "pokemon"
    asset := "pikachu"
    version := "red"

    src, err := setupSourceForTransferDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()

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
    err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
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

    src, err := setupSourceForTransferDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()

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
        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
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
        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{ IgnoreDot: true })
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

    // Executing the transfer; by default, nothing is moved, until Consume=true.
    t.Run("no consume", func(t *testing.T) {
        src, err := setupSourceForTransferDirectoryTest()
        if err != nil {
            t.Fatalf("failed to set up test directories; %v", err)
        }

        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the registry; %v", err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
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
        src, err := setupSourceForTransferDirectoryTest()
        if err != nil {
            t.Fatalf("failed to set up test directories; %v", err)
        }

        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the registry; %v", err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{ Consume: true })
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
        src, err := setupSourceForTransferDirectoryTest()
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

        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{ Consume: true })
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

    src, err := setupSourceForTransferDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Executing the first transfer.
    {
        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
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
        err = transferDirectory(src, reg, project, asset, new_version, ctx, transferDirectoryOptions{})
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
        err = transferDirectory(src, reg, project, asset, new_version, ctx, transferDirectoryOptions{})
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
        err = transferDirectory(src, reg, project, asset, new_version, ctx, transferDirectoryOptions{})
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

func TestTransferDirectoryRegistryLinks(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()

    src, err := setupSourceForTransferDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Mocking up a directory structure and executing a series of transfers to create appropriate links. 
    {
        project := "pokemon"
        asset := "pikachu"

        err = transferDirectory(src, reg, project, asset, "red", ctx, transferDirectoryOptions{})
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

        err = transferDirectory(src, reg, project, asset, "blue", ctx, transferDirectoryOptions{})
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

        err = transferDirectory(src, reg, project, asset, "green", ctx, transferDirectoryOptions{})
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
        err = transferDirectory(src, reg, project, asset, "kanto", ctx, transferDirectoryOptions{})
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
        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
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

        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "currently-transferring") {
            t.Errorf("expected a failure when linking to the currently-transferring version; %v", err)
        }
    })

    t.Run("probational versions", func(t *testing.T) {
        src, err := setupSourceForTransferDirectoryTest()
        if err != nil {
            t.Fatalf("failed to set up test directories; %v", err)
        }

        project := "POKEMON"
        asset := "GYARADOS"
        version := "yellow"
        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
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
        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "probational") {
            t.Errorf("expected a failure when linking to a probational version; %v", err)
        }
    })

    t.Run("internal files", func(t *testing.T) {
        src, err := setupSourceForTransferDirectoryTest()
        if err != nil {
            t.Fatalf("failed to set up test directories; %v", err)
        }

        project := "POKEMON"
        asset := "MACHOMP"
        version := "red"
        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
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
        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
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
        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "is a directory") {
            t.Errorf("expected a failure when a symbolic link to a directory is present; %v", err)
        }
    })
}

func TestTransferDirectoryLocalLinks(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()

    src, err := setupSourceForTransferDirectoryTest()
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

    err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
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

    t.Run("cyclic links", func(t* testing.T) {
        src, err := setupSourceForTransferDirectoryTest()
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

        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "too many levels") {
            t.Errorf("failed to detect cyclic local links; %v", err)
        }
    })

    t.Run("directory links", func(t *testing.T) {
        src, err := setupSourceForTransferDirectoryTest()
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

        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "is a directory") {
            t.Errorf("failed to detect links to a directory; %v", err)
        }
    })
}

func TestTransferDirectoryExternalLinks(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()

    src, err := setupSourceForTransferDirectoryTest()
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
        err := transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
        if err == nil && !strings.Contains(err.Error(), "not allowed") {
            t.Error("expected a failure when symlinking to a non-whitelisted directory")
        }
    })

    // Now with whitelisting.
    t.Run("whitelist", func(t *testing.T) {
        project := "POKEMON"
        asset := "PIKAPIKA"
        version := "SILVER"
        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{ LinkWhitelist: []string{ filepath.Dir(other_name) } })
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

/**********************************************
 **********************************************/

func TestReindexDirectorySimple(t *testing.T) {
    project := "pokemon"
    asset := "pikachu"
    version := "red"

    src, err := setupSourceForTransferDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    ctx := context.Background()

    // Setting up the transfer.
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
    if err != nil {
        t.Fatalf("failed to perform the transfer; %v", err)
    }

    v_path := filepath.Join(reg, project, asset, version)
    prior, err := loadDirectoryContents(v_path)
    if err != nil {
        t.Fatalf("failed to load directory contents; %v", err)
    }

    // Now reindexing after we purge all the '..xxx' files.
    err = stripDoubleDotFiles(v_path)
    if err != nil {
        t.Fatalf("failed to strip all double dots; %v", err)
    }

    err = reindexDirectory(reg, project, asset, version, ctx, reindexDirectoryOptions{})
    if err != nil {
        t.Fatalf("failed to reindex directory; %v", err)
    }

    recovered, err := loadDirectoryContents(v_path)
    if err != nil {
        t.Fatalf("failed to load directory contents; %v", err)
    }
    err = compareDirectoryContents(prior, recovered)
    if err != nil {
        t.Error(err)
    }
}

func TestReindexDirectorySkipInternal(t *testing.T) {
    project := "pokemon"
    asset := "pikachu"
    version := "red"

    src, err := setupSourceForTransferDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Setting up the transfer.
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()

    err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
    if err != nil {
        t.Fatalf("failed to perform the transfer; %v", err)
    }

    // Injecting some hidden files.
    dest := filepath.Join(reg, project, asset, version)
    err = os.Mkdir(filepath.Join(dest, "..cache"), 0755)
    if err != nil {
        t.Fatalf("failed to make a hidden directory; %v", err)
    }
    err = os.WriteFile(filepath.Join(dest, "..cache", "foo"), []byte{}, 0644)
    if err != nil {
        t.Fatalf("failed to write file inside a hidden directory; %v", err)
    }

    err = os.WriteFile(filepath.Join(dest, "..special"), []byte{}, 0644)
    if err != nil {
        t.Fatalf("failed to write hidden file; %v", err)
    }

    v_path := filepath.Join(reg, project, asset, version)
    prior, err := loadDirectoryContents(v_path)
    if err != nil {
        t.Fatalf("failed to load directory contents; %v", err)
    }

    // Now reindexing after without purging all the internal files.
    err = reindexDirectory(reg, project, asset, version, ctx, reindexDirectoryOptions{})
    if err != nil {
        t.Fatalf("failed to reindex directory; %v", err)
    }

    recovered, err := loadDirectoryContents(v_path)
    if err != nil {
        t.Fatalf("failed to load directory contents; %v", err)
    }
    err = compareDirectoryContents(prior, recovered)
    if err != nil {
        t.Error(err)
    }

    // Checking that the dot files are still there, but not indexed.
    if _, ok := recovered["..cache/foo"]; !ok {
        t.Error(".cache/foo should still be present in the directory")
    }
    if _, ok := recovered["..special"]; !ok {
        t.Error(".special should still be present in the directory")
    }

    man, err := readManifest(dest)
    if err != nil {
        t.Fatal(err)
    }

    for k, _ := range man {
        if strings.HasPrefix(filepath.Base(k), "..") {
            t.Error("dot files should not be present in the new manifest")
        }
    }
}

func TestReindexDirectoryRegistryLinks(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()

    src, err := setupSourceForTransferDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Mocking up a directory structure and executing a series of transfers to create appropriate links. 
    project := "pokemon"
    asset := "pikachu"

    {
        err = transferDirectory(src, reg, project, asset, "red", ctx, transferDirectoryOptions{})
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

        err = transferDirectory(src, reg, project, asset, "blue", ctx, transferDirectoryOptions{})
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

        err = transferDirectory(src, reg, project, asset, "green", ctx, transferDirectoryOptions{})
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

    {
        version := "blue"
        v_path := filepath.Join(reg, project, asset, version)
        prior, err := loadDirectoryContents(v_path)
        if err != nil {
            t.Fatalf("failed to load directory contents; %v", err)
        }

        // Now reindexing after we purge all the '..xxx' files.
        err = stripDoubleDotFiles(v_path)
        if err != nil {
            t.Fatalf("failed to strip all double dots; %v", err)
        }
        err = os.WriteFile(filepath.Join(v_path, summaryFileName), []byte("{}"), 0644) // mocking this up for a valid comparison.
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }

        err = reindexDirectory(reg, project, asset, version, ctx, reindexDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to reindex directory; %v", err)
        }

        recovered, err := loadDirectoryContents(v_path)
        if err != nil {
            t.Fatalf("failed to load directory contents; %v", err)
        }
        err = compareDirectoryContents(prior, recovered)
        if err != nil {
            t.Error(err)
        }

        // Confirming that we have ..links files.
        _, found := recovered[linksFileName]
        if !found {
            t.Error("missing a top-level ..links file")
        }

        _, found = recovered[filepath.Join("moves", "electric", linksFileName)]
        if !found {
            t.Error("missing a nested ..links file")
        }
    }

    // Checking that reindexing preserves ancestral information if ..links are present.
    {
        version := "green"
        v_path := filepath.Join(reg, project, asset, version)
        prior, err := loadDirectoryContents(v_path)
        if err != nil {
            t.Fatalf("failed to load directory contents; %v", err)
        }

        // Don't delete all the ..links files.
        err = os.Remove(filepath.Join(v_path, manifestFileName))
        if err != nil {
            t.Fatal(err)
        }

        err = reindexDirectory(reg, project, asset, version, ctx, reindexDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to reindex directory; %v", err)
        }

        recovered, err := loadDirectoryContents(v_path)
        if err != nil {
            t.Fatalf("failed to load directory contents; %v", err)
        }
        err = compareDirectoryContents(prior, recovered)
        if err != nil {
            t.Error(err)
        }
    }
}

func TestReindexDirectoryRegistryLinkFailures(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()

    src, err := setupSourceForTransferDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    t.Run("external files", func(t *testing.T) {
        project := "pokemon"
        asset := "lugia"
        version := "silver"
        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }

        other, err := os.CreateTemp("", "")
        if err != nil {
            t.Fatalf("failed to create a random temporary file; %v", err)
        }
        if _, err := other.WriteString("gotta catch em all"); err != nil {
            t.Fatalf("failed to write a random temporary file; %v", err)
        }
        other_name := other.Name()
        if err := other.Close(); err != nil {
            t.Fatalf("failed to close a random temporary file; %v", err)
        }

        v_path := filepath.Join(reg, project, asset, version)
        err = os.Symlink(other_name, filepath.Join(v_path, "asdasd"))
        if err != nil {
            t.Fatalf("failed to create a test link to a random file")
        }

        err = stripDoubleDotFiles(v_path)
        if err != nil {
            t.Fatalf("failed to strip all double dots; %v", err)
        }

        err = reindexDirectory(reg, project, asset, version, ctx, reindexDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "outside the registry") {
            t.Errorf("expected reindexing failure for files outside the registry; %v", err)
        }
    })

    // All other failures are handled by resolveLocalSymlink and are common to
    // both transfer and reindex functions, so we won't test them again here.
}

func TestReindexDirectoryLocalLinks(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()

    src, err := setupSourceForTransferDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    err = os.Symlink(filepath.Join(src, "type"), filepath.Join(src, "type2"))
    if err != nil {
        t.Fatalf("failed to create a symlink for 'types2'; %v", err)
    }

    err = os.Symlink(filepath.Join("..", "type2"), filepath.Join(src, "evolution", "foo")) // relative symlink to another symlink.
    if err != nil {
        t.Fatalf("failed to create a symlink for 'evolution/foo'; %v", err)
    }

    t.Run("simple", func(t *testing.T) {
        project := "POKEMON"
        asset := "PIKAPIKA"
        version := "GOLD"

        err := transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }

        v_path := filepath.Join(reg, project, asset, version)
        prior, err := loadDirectoryContents(v_path)
        if err != nil {
            t.Fatalf("failed to load directory contents; %v", err)
        }

        err = stripDoubleDotFiles(v_path)
        if err != nil {
            t.Fatalf("failed to strip all double dots; %v", err)
        }

        err = reindexDirectory(reg, project, asset, version, ctx, reindexDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        recovered, err := loadDirectoryContents(v_path)
        if err != nil {
            t.Fatalf("failed to load directory contents; %v", err)
        }

        // Same link information is reconstituted for 'type2'.
        if recovered["type2"].LinkTarget != "type" || recovered[linksFileName].Contents != prior[linksFileName].Contents {
            t.Error("mismatch in link information for 'type2'")
        }

        // But we lose the immediate parent link information for 'evolution/foo'.
        if recovered["evolution/foo"].LinkTarget != "../type" {
            t.Error("unexpected link target for 'evolution/foo'")
        }
    })

    t.Run("with ..links", func(t *testing.T) {
        project := "POKEMON"
        asset := "PIKAPIKA"
        version := "SILVER"
        err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }

        v_path := filepath.Join(reg, project, asset, version)
        prior, err := loadDirectoryContents(v_path)
        if err != nil {
            t.Fatalf("failed to load directory contents; %v", err)
        }

        err = stripDoubleDotFiles(v_path)
        if err != nil {
            t.Fatalf("failed to strip all double dots; %v", err)
        }

        // Inject a ..link file back in, and removing the link itself to ensure that reindexing doesn't rely on existing links if ..links is present.
        err = os.WriteFile(filepath.Join(v_path, linksFileName), []byte(fmt.Sprintf(`{
    "type2": {
        "project": "%s",
        "asset": "%s",
        "version": "%s",
        "path": "type"
    }
}`, project, asset, version)), 0644)
        if err != nil {
            t.Fatal(err)
        }
        err = os.Remove(filepath.Join(v_path, "type2"))
        if err != nil {
            t.Fatal(err)
        }

        // Inject a ..link file with ancestral information.
        err = os.WriteFile(filepath.Join(v_path, "evolution", linksFileName), []byte(fmt.Sprintf(`{
    "foo": {
        "project": "%s",
        "asset": "%s",
        "version": "%s",
        "path": "type2",
        "ancestor": {
            "project": "%s",
            "asset": "%s",
            "version": "%s",
            "path": "type"
        }
    }
}`, project, asset, version, project, asset, version)), 0644)
        if err != nil {
            t.Fatal(err)
        }

        err = reindexDirectory(reg, project, asset, version, ctx, reindexDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        recovered, err := loadDirectoryContents(v_path)
        if err != nil {
            t.Fatalf("failed to load directory contents; %v", err)
        }

        err = compareDirectoryContents(prior, recovered)
        if err != nil {
            t.Error(err)
        }
    })

    // All failures are handled by resolveLocalSymlink and are common to
    // both transfer and reindex functions, so we won't test them again here.
}

func TestReindexDirectoryLinkWhitelist(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()

    src, err := setupSourceForTransferDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Mocking up a directory structure. 
    project := "pokemon"
    asset := "lugia"
    version := "silver"
    err = transferDirectory(src, reg, project, asset, version, ctx, transferDirectoryOptions{})
    if err != nil {
        t.Fatalf("failed to perform the transfer; %v", err)
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

    v_path := filepath.Join(reg, project, asset, version)
    err = os.Symlink(other_name, filepath.Join(v_path, "asdasd"))
    if err != nil {
        t.Fatalf("failed to create a test link to a random file")
    }

    err = stripDoubleDotFiles(v_path)
    if err != nil {
        t.Fatalf("failed to strip all double dots; %v", err)
    }

    err = reindexDirectory(reg, project, asset, version, ctx, reindexDirectoryOptions{ LinkWhitelist: []string{ filepath.Dir(other_name) } })
    if err != nil { 
        t.Fatal(err)
    }

    man, err := readManifest(v_path)
    if err != nil {
        t.Fatal(err)
    }

    contents, found := man["asdasd"]
    if !found || contents.Link != nil || contents.Size != int64(len(message)) {
        t.Error("unexpected manifest entry for whitelisted symlink")
    }
}
