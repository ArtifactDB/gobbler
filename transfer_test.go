package main

import (
    "testing"
    "os"
    "path/filepath"
    "io/ioutil"
    "fmt"
    "strings"
    "errors"
)

func setup_source_and_registry(project, asset, version string) (string, string, error) {
    reg, err := ioutil.TempDir("", "")
    if err != nil {
        return "", "", fmt.Errorf("failed to create the registry; %w", err)
    }

    dir, err := ioutil.TempDir("", "")
    if err != nil {
        return "", "", fmt.Errorf("failed to create the temporary directory; %w", err)
    }

    target := filepath.Join(reg, project, asset, version)
    err = os.MkdirAll(target, 0755)
    if err != nil {
        return "", "", fmt.Errorf("failed to create the version directory; %w", err)
    }

    return reg, dir, nil
}

func mock_source(src string) error {
    err := os.WriteFile(filepath.Join(src, "type"), []byte("electric"), 0644)
    if err != nil {
        return err
    }

    err = os.Mkdir(filepath.Join(src, "evolution"), 0755)
    if err != nil {
        return err
    }
    err = os.WriteFile(filepath.Join(src, "evolution", "up"), []byte("raichu"), 0644)
    if err != nil {
        return err
    }
    err = os.WriteFile(filepath.Join(src, "evolution", "down"), []byte("pichu"), 0644)
    if err != nil {
        return err
    }

    err = os.Mkdir(filepath.Join(src, "moves"), 0755)
    if err != nil {
        return err
    }
    err = os.Mkdir(filepath.Join(src, "moves", "electric"), 0755)
    if err != nil {
        return err
    }
    err = os.WriteFile(filepath.Join(src, "moves", "electric", "thunder_shock"), []byte("40"), 0644)
    if err != nil {
        return err
    }
    err = os.WriteFile(filepath.Join(src, "moves", "electric", "thunderbolt"), []byte("90"), 0644)
    if err != nil {
        return err
    }
    err = os.WriteFile(filepath.Join(src, "moves", "electric", "thunder"), []byte("110"), 0644)
    if err != nil {
        return err
    }
    err = os.Mkdir(filepath.Join(src, "moves", "normal"), 0755)
    if err != nil {
        return err
    }
    err = os.WriteFile(filepath.Join(src, "moves", "normal", "quick_attack"), []byte("40"), 0644)
    if err != nil {
        return err
    }
    err = os.WriteFile(filepath.Join(src, "moves", "normal", "double_team"), []byte("0"), 0644)
    if err != nil {
        return err
    }

    return nil
}

func verify_file_contents(path, contents string) error {
    observed, err := os.ReadFile(path)
    if err != nil {
        return fmt.Errorf("failed to read %q; %w", path, err)
    }
    if string(observed) != contents {
        return fmt.Errorf("unexpected contents of %q; %w", path, err)
    }
    return nil
}

func TestTransferSimple(t *testing.T) {
    project := "pokemon"
    asset := "pikachu"
    version := "red"

    reg, src, err := setup_source_and_registry(project, asset, version)
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Mocking up a directory structure.
    err = mock_source(src)
    if err != nil {
        t.Fatalf("failed to mock up source contents; %v", err)
    }

    // Executing the transfer.
    err = Transfer(src, reg, project, asset, version)
    if err != nil {
        t.Fatalf("failed to perform the transfer; %v", err)
    }

    // Checking a few manifest entries...
    destination := filepath.Join(reg, project, asset, version)
    man, err := ReadManifest(destination)
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
    err = verify_file_contents(filepath.Join(destination, "type"), "electric")
    if err != nil {
        t.Fatal(err)
    }
    err = verify_file_contents(filepath.Join(destination, "evolution", "down"), "pichu")
    if err != nil {
        t.Fatal(err)
    }
    err = verify_file_contents(filepath.Join(destination, "moves", "normal", "double_team"), "0")
    if err != nil {
        t.Fatal(err)
    }
}

func extract_symlink_target(path string) (string, error) {
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

func verify_symlink(manifest map[string]ManifestEntry, version_dir, path, contents, target_project, target_asset, target_version, target_path string, has_ancestor bool) error {
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
    err := verify_file_contents(full, contents)
    if err != nil {
        return err
    }

    target, err := extract_symlink_target(full)
    if err != nil {
        return err
    }
    if !strings.HasPrefix(target, "../") || !strings.HasSuffix(target, "/" + target_project + "/" + target_asset + "/" + target_version + "/" + target_path) {
        return fmt.Errorf("unexpected symlink target for %q (got %q)", path, target)
    }

    return nil
}

func verify_not_symlink(manifest map[string]ManifestEntry, version_dir, path, contents string) error {
    info, ok := manifest[path]
    if !ok || int(info.Size) != len(contents) || info.Link != nil {
        return fmt.Errorf("unexpected manifest entry for %q", path)
    }

    full := filepath.Join(version_dir, path)
    err := verify_file_contents(full, contents)
    if err != nil {
        return err
    }

    return nil
}

func verify_ancestral_symlink(
    manifest map[string]ManifestEntry, 
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

    reg, src, err := setup_source_and_registry(project, asset, version)
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Mocking up a directory structure and executing the first transfer.
    {
        err = mock_source(src)
        if err != nil {
            t.Fatalf("failed to mock up source contents; %v", err)
        }

        err = Transfer(src, reg, project, asset, version)
        if err != nil {
            t.Fatalf("failed to perform the transfer; %v", err)
        }

        err = os.WriteFile(filepath.Join(reg, project, asset, LatestFileName), []byte("{ \"latest\": \"" + version + "\" }"), 0644)
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

        err = os.WriteFile(filepath.Join(reg, project, asset, LatestFileName), []byte("{ \"latest\": \"" + new_version + "\" }"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }

        destination := filepath.Join(reg, project, asset, new_version)
        man, err := ReadManifest(destination)
        if err != nil {
            t.Fatalf("failed to read the manifest; %v", err)
        }

        // Different file name.
        err = verify_symlink(man, destination, "evolution/next", "raichu", project, asset, version, "evolution/up", false)
        if err != nil {
            t.Fatal(err)
        }

        // Same file name.
        err = verify_symlink(man, destination, "moves/electric/thunder", "110", project, asset, version, "moves/electric/thunder", false)
        if err != nil {
            t.Fatal(err)
        }

        // Modified file.
        err = verify_not_symlink(man, destination, "moves/electric/thunder_shock", "some_different_value")
        if err != nil {
            t.Fatal(err)
        }

        // New file.
        err = verify_not_symlink(man, destination, "moves/steel/iron_tail", "100")
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

        err = os.WriteFile(filepath.Join(reg, project, asset, LatestFileName), []byte("{ \"latest\": \"" + new_version + "\" }"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }

        destination := filepath.Join(reg, project, asset, new_version)
        man, err := ReadManifest(destination)
        if err != nil {
            t.Fatalf("failed to read the manifest; %v", err)
        }

        err = verify_symlink(man, destination, "evolution/final", "raichu", project, asset, "blue", "evolution/next", true)
        if err != nil {
            t.Fatal(err)
        }
        err = verify_ancestral_symlink(man, destination, "evolution/final", reg, project, asset, "red", "evolution/up") 
        if err != nil {
            t.Fatal(err)
        }

        err = verify_symlink(man, destination, "moves/electric/thunderbolt", "90", project, asset, "blue", "moves/electric/thunderbolt", true)
        if err != nil {
            t.Fatal(err)
        }
        err = verify_ancestral_symlink(man, destination, "moves/electric/thunderbolt", reg, project, asset, "red", "moves/electric/thunderbolt") 
        if err != nil {
            t.Fatal(err)
        }

        err = verify_not_symlink(man, destination, "moves/electric/thunder_shock", "9999")
        if err != nil {
            t.Fatal(err)
        }

        err = verify_not_symlink(man, destination, "moves/normal/feint", "30")
        if err != nil {
            t.Fatal(err)
        }

        err = verify_symlink(man, destination, "moves/steel/iron_tail", "100", project, asset, "blue", "moves/steel/iron_tail", false)
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

        err = os.WriteFile(filepath.Join(reg, project, asset, LatestFileName), []byte("{ \"latest\": \"" + new_version + "\" }"), 0644)
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }

        destination := filepath.Join(reg, project, asset, new_version)
        man, err := ReadManifest(destination)
        if err != nil {
            t.Fatalf("failed to read the manifest; %v", err)
        }

        err = verify_symlink(man, destination, "evolution/final", "raichu", project, asset, "green", "evolution/final", true)
        if err != nil {
            t.Fatal(err)
        }
        err = verify_ancestral_symlink(man, destination, "evolution/final", reg, project, asset, "red", "evolution/up") 
        if err != nil {
            t.Fatal(err)
        }

        err = verify_symlink(man, destination, "moves/electric/thunder_shock", "9999", project, asset, "green", "moves/electric/thunder_shock", false)
        if err != nil {
            t.Fatal(err)
        }

        // We can also form new ancestral links.
        err = verify_symlink(man, destination, "moves/steel/iron_tail", "100", project, asset, "green", "moves/steel/iron_tail", true)
        if err != nil {
            t.Fatal(err)
        }
        err = verify_ancestral_symlink(man, destination, "moves/steel/iron_tail", reg, project, asset, "blue", "moves/steel/iron_tail")
        if err != nil {
            t.Fatal(err)
        }
    }
}

