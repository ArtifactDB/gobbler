package main

import (
    "testing"
    "os"
    "path/filepath"
    "fmt"
    "strings"
    "errors"
    "encoding/json"
)

func setupSourceForTransferTest() (string, error) {
    src, err := os.MkdirTemp("", "")
    if err != nil {
        return "", fmt.Errorf("failed to create the temporary directory; %w", err)
    }

    err = os.WriteFile(filepath.Join(src, "type"), []byte("electric"), 0644)
    if err != nil {
        return "", err
    }

    err = os.Mkdir(filepath.Join(src, "evolution"), 0755)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(src, "evolution", "up"), []byte("raichu"), 0644)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(src, "evolution", "down"), []byte("pichu"), 0644)
    if err != nil {
        return "", err
    }

    err = os.Mkdir(filepath.Join(src, "moves"), 0755)
    if err != nil {
        return "", err
    }
    err = os.Mkdir(filepath.Join(src, "moves", "electric"), 0755)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(src, "moves", "electric", "thunder_shock"), []byte("40"), 0644)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(src, "moves", "electric", "thunderbolt"), []byte("90"), 0644)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(src, "moves", "electric", "thunder"), []byte("110"), 0644)
    if err != nil {
        return "", err
    }

    err = os.Mkdir(filepath.Join(src, "moves", "normal"), 0755)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(src, "moves", "normal", "quick_attack"), []byte("40"), 0644)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(src, "moves", "normal", "double_team"), []byte("0"), 0644)
    if err != nil {
        return "", err
    }

    return src, nil
}

func TestTransferSimple(t *testing.T) {
    project := "pokemon"
    asset := "pikachu"
    version := "red"

    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    src, err := setupSourceForTransferTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Executing the transfer.
    err = Transfer(src, reg, project, asset, version)
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
        t.Fatalf("unexpected manifest entry for 'evolution/up'")
    }
    info, ok = man["moves/electric/thunder"]
    if !ok || int(info.Size) != len("110") || info.Link != nil {
        t.Fatalf("unexpected manifest entry for 'moves/electric/thunder'")
    }

    // Checking some of the actual files.
    err = verifyFileContents(filepath.Join(destination, "type"), "electric")
    if err != nil {
        t.Fatal(err)
    }
    err = verifyFileContents(filepath.Join(destination, "evolution", "down"), "pichu")
    if err != nil {
        t.Fatal(err)
    }
    err = verifyFileContents(filepath.Join(destination, "moves", "normal", "double_team"), "0")
    if err != nil {
        t.Fatal(err)
    }
}

func TestTransferSkipHidden(t *testing.T) {
    project := "pokemon"
    asset := "pikachu"
    version := "red"

    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    src, err := setupSourceForTransferTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Injecting some hidden files.
    err = os.WriteFile(filepath.Join(src, ".DS_store"), []byte("some_mac_crap"), 0644)
    if err != nil {
        t.Fatalf("failed to write hidden file; %v", err)
    }

    err = os.Mkdir(filepath.Join(src, ".cache"), 0755)
    if err != nil {
        t.Fatalf("failed to make a hidden directory; %v", err)
    }

    err = os.WriteFile(filepath.Join(src, ".cache", "credentials"), []byte("password"), 0644)
    if err != nil {
        t.Fatalf("failed to write file inside a hidden directory; %v", err)
    }

    // Executing the transfer.
    err = Transfer(src, reg, project, asset, version)
    if err != nil {
        t.Fatalf("failed to perform the transfer; %v", err)
    }

    destination := filepath.Join(reg, project, asset, version)
    if _, err := os.Stat(filepath.Join(destination, ".DS_store")); !errors.Is(err, os.ErrNotExist) {
        t.Fatal("hidden files should not be transferred")
    }

    if _, err := os.Stat(filepath.Join(destination, ".cache", "credentials")); !errors.Is(err, os.ErrNotExist) {
        t.Fatal("hidden files should not be transferred")
    }
}

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

func verifySymlink(manifest map[string]manifestEntry, version_dir, path, contents, target_project, target_asset, target_version, target_path string, has_ancestor bool) error {
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
    if !strings.HasPrefix(target, "../") || !strings.HasSuffix(target, "/" + target_project + "/" + target_asset + "/" + target_version + "/" + target_path) {
        return fmt.Errorf("unexpected symlink target for %q (got %q)", path, target)
    }

    {
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
    }

    return nil
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

func TestTransferDeduplication(t *testing.T) {
    project := "pokemon"
    asset := "pikachu"
    version := "red"

    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    src, err := setupSourceForTransferTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Executing the first transfer.
    {
        err = Transfer(src, reg, project, asset, version)
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }

        err = os.WriteFile(filepath.Join(reg, project, asset, latestFileName), []byte("{ \"latest\": \"" + version + "\" }"), 0644)
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
        err = Transfer(src, reg, project, asset, new_version)
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }

        err = os.WriteFile(filepath.Join(reg, project, asset, latestFileName), []byte("{ \"latest\": \"" + new_version + "\" }"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }

        destination := filepath.Join(reg, project, asset, new_version)
        man, err := readManifest(destination)
        if err != nil {
            t.Fatalf("failed to read the manifest; %v", err)
        }

        // Different file name.
        err = verifySymlink(man, destination, "evolution/next", "raichu", project, asset, version, "evolution/up", false)
        if err != nil {
            t.Fatal(err)
        }

        // Same file name.
        err = verifySymlink(man, destination, "moves/electric/thunder", "110", project, asset, version, "moves/electric/thunder", false)
        if err != nil {
            t.Fatal(err)
        }

        // Modified file.
        err = verifyNotSymlink(man, destination, "moves/electric/thunder_shock", "some_different_value")
        if err != nil {
            t.Fatal(err)
        }

        // New file.
        err = verifyNotSymlink(man, destination, "moves/steel/iron_tail", "100")
        if err != nil {
            t.Fatal(err)
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
        err = Transfer(src, reg, project, asset, new_version)
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }

        err = os.WriteFile(filepath.Join(reg, project, asset, latestFileName), []byte("{ \"latest\": \"" + new_version + "\" }"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }

        destination := filepath.Join(reg, project, asset, new_version)
        man, err := readManifest(destination)
        if err != nil {
            t.Fatalf("failed to read the manifest; %v", err)
        }

        err = verifySymlink(man, destination, "evolution/final", "raichu", project, asset, "blue", "evolution/next", true)
        if err != nil {
            t.Fatal(err)
        }
        err = verifyAncestralSymlink(man, destination, "evolution/final", reg, project, asset, "red", "evolution/up") 
        if err != nil {
            t.Fatal(err)
        }

        err = verifySymlink(man, destination, "moves/electric/thunderbolt", "90", project, asset, "blue", "moves/electric/thunderbolt", true)
        if err != nil {
            t.Fatal(err)
        }
        err = verifyAncestralSymlink(man, destination, "moves/electric/thunderbolt", reg, project, asset, "red", "moves/electric/thunderbolt") 
        if err != nil {
            t.Fatal(err)
        }

        err = verifyNotSymlink(man, destination, "moves/electric/thunder_shock", "9999")
        if err != nil {
            t.Fatal(err)
        }

        err = verifyNotSymlink(man, destination, "moves/normal/feint", "30")
        if err != nil {
            t.Fatal(err)
        }

        err = verifySymlink(man, destination, "moves/steel/iron_tail", "100", project, asset, "blue", "moves/steel/iron_tail", false)
        if err != nil {
            t.Fatal(err)
        }
    }

    // Executing the transfer AGAIN to that ancestral links of the older version are themselves respected.
    {
        new_version := "yellow"
        err = Transfer(src, reg, project, asset, new_version)
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }

        err = os.WriteFile(filepath.Join(reg, project, asset, latestFileName), []byte("{ \"latest\": \"" + new_version + "\" }"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }

        destination := filepath.Join(reg, project, asset, new_version)
        man, err := readManifest(destination)
        if err != nil {
            t.Fatalf("failed to read the manifest; %v", err)
        }

        err = verifySymlink(man, destination, "evolution/final", "raichu", project, asset, "green", "evolution/final", true)
        if err != nil {
            t.Fatal(err)
        }
        err = verifyAncestralSymlink(man, destination, "evolution/final", reg, project, asset, "red", "evolution/up") 
        if err != nil {
            t.Fatal(err)
        }

        err = verifySymlink(man, destination, "moves/electric/thunder_shock", "9999", project, asset, "green", "moves/electric/thunder_shock", false)
        if err != nil {
            t.Fatal(err)
        }

        // We can also form new ancestral links.
        err = verifySymlink(man, destination, "moves/steel/iron_tail", "100", project, asset, "green", "moves/steel/iron_tail", true)
        if err != nil {
            t.Fatal(err)
        }
        err = verifyAncestralSymlink(man, destination, "moves/steel/iron_tail", reg, project, asset, "blue", "moves/steel/iron_tail")
        if err != nil {
            t.Fatal(err)
        }
    }
}

func TestTransferLinks(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    src, err := setupSourceForTransferTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Mocking up a directory structure and executing a series of transfers to create appropriate links. 
    {
        project := "pokemon"
        asset := "pikachu"

        err = Transfer(src, reg, project, asset, "red")
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }
        err = os.WriteFile(filepath.Join(reg, project, asset, "red", summaryFileName), []byte("{}"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }
        err = os.WriteFile(filepath.Join(reg, project, asset, latestFileName), []byte("{ \"latest\": \"red\" }"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }

        err = Transfer(src, reg, project, asset, "blue")
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }
        err = os.WriteFile(filepath.Join(reg, project, asset, "blue", summaryFileName), []byte("{}"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }
        err = os.WriteFile(filepath.Join(reg, project, asset, latestFileName), []byte("{ \"latest\": \"blue\" }"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }

        err = Transfer(src, reg, project, asset, "green")
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }
        err = os.WriteFile(filepath.Join(reg, project, asset, "green", summaryFileName), []byte("{}"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }
        err = os.WriteFile(filepath.Join(reg, project, asset, latestFileName), []byte("{ \"latest\": \"green\" }"), 0644)
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
        err = Transfer(src, reg, project, asset, "kanto")
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }

        destination := filepath.Join(reg, project, asset, "kanto")
        man, err := readManifest(destination)
        if err != nil {
            t.Fatalf("failed to read the manifest; %v", err)
        }

        err = verifySymlink(man, destination, "types/first", "electric", "pokemon", "pikachu", "red", "type", false)
        if err != nil {
            t.Fatal(err)
        }

        err = verifySymlink(man, destination, "moves/electric/THUNDERBOLT", "90", "pokemon", "pikachu", "blue", "moves/electric/thunderbolt", true)
        if err != nil {
            t.Fatal(err)
        }
        err = verifyAncestralSymlink(man, destination, "moves/electric/THUNDERBOLT", reg, "pokemon", "pikachu", "red", "moves/electric/thunderbolt")
        if err != nil {
            t.Fatal(err)
        }

        err = verifySymlink(man, destination, "best_friend", "pichu", "pokemon", "pikachu", "green", "evolution/down", true)
        if err != nil {
            t.Fatal(err)
        }
        err = verifyAncestralSymlink(man, destination, "best_friend", reg, "pokemon", "pikachu", "red", "evolution/down")
        if err != nil {
            t.Fatal(err)
        }

        err = verifyNotSymlink(man, destination, "types/second", "steel")
        if err != nil {
            t.Fatal(err)
        }
    }
}

func TestTransferLinkFailures(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    // Links to irrelevant files are copied.
    {
        src, err := setupSourceForTransferTest()
        if err != nil {
            t.Fatalf("failed to set up test directories; %v", err)
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

        err = os.Symlink(other_name, filepath.Join(src, "asdasd"))
        if err != nil {
            t.Fatalf("failed to create a test link to a random file")
        }

        project := "POKEMON"
        asset := "PIKAPIKA"
        version := "SILVER"
        err = Transfer(src, reg, project, asset, version)
        if err != nil {
            t.Fatal(err)
        }

        destination := filepath.Join(reg, project, asset, version)
        man, err := readManifest(destination)
        if err != nil {
            t.Fatalf("failed to read the manifest; %v", err)
        }

        err = verifyNotSymlink(man, destination, "asdasd", "gotta catch em all")
        if err != nil {
            t.Fatal(err)
        }
    }

    // Links to loose files in the registry are forbidden.
    {
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
        err = Transfer(src, reg, project, asset, version)
        if err == nil || !strings.Contains(err.Error(), "outside of a project asset version directory") {
            t.Fatal(err)
        }
    }

    // Links to the currently transferring project are forbidden.
    {
        src, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        project := "POKEMON"
        asset := "PIKAPIKA"
        version := "GOLD"

        err = os.WriteFile(filepath.Join(src, "aaa"), []byte("123123123123"), 0644)
        if err != nil {
            t.Fatalf("failed to mock up a random file")
        }

        // Deliberately 'zzz' to sort after 'aaa' so that it gets walked later, otherwise the link target doesn't exist yet!
        err = os.Symlink(filepath.Join(reg, project, asset, version, "aaa"), filepath.Join(src, "zzz")) 
        if err != nil {
            t.Fatalf("failed to create a test link to a random file")
        }

        err = Transfer(src, reg, project, asset, version)
        if err == nil || !strings.Contains(err.Error(), "currently-transferring") {
            t.Fatal(err)
        }
    }

    // Links to probational versions are forbidden.
    {
        src, err := setupSourceForTransferTest()
        if err != nil {
            t.Fatalf("failed to set up test directories; %v", err)
        }

        project := "POKEMON"
        asset := "GYARADOS"
        version := "yellow"
        err = Transfer(src, reg, project, asset, version)
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
        err = Transfer(src, reg, project, asset, version)
        if err == nil || !strings.Contains(err.Error(), "probational") {
            t.Fatal(err)
        }
    }

    // Links to internal files are forbidden.
    {
        src, err := setupSourceForTransferTest()
        if err != nil {
            t.Fatalf("failed to set up test directories; %v", err)
        }

        project := "POKEMON"
        asset := "MACHOMP"
        version := "red"
        err = Transfer(src, reg, project, asset, version)
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
        err = Transfer(src, reg, project, asset, version)
        if err == nil || !strings.Contains(err.Error(), "internal '..' files") {
            t.Fatal(err)
        }
    }

    // Links to loose files in the registry are forbidden.
    {
        src, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        mock, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory as a link target; %v", err)
        }

        if err := os.Symlink(mock, filepath.Join(src, "WHEE")); err != nil {
            t.Fatalf("failed to make a symlink to the mock directory; %v", err)
        }

        project := "POKEMON"
        asset := "VILEPLUME"
        version := "green"
        err = Transfer(src, reg, project, asset, version)
        if err == nil || !strings.Contains(err.Error(), "symbolic links to directories") {
            t.Fatal("expected a failure when a symbolic link to a directory is present")
        }
    }
}
