package main

import (
    "testing"
    "os"
    "path/filepath"
    "strings"
    "context"
)

func setupSourceForValidateDirectoryTest() (string, error) {
    dir, err := setupSourceForTransferDirectoryTest()
    return dir, err
}

func TestValidateDirectorySimple(t *testing.T) {
    src, err := setupSourceForValidateDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    ctx := context.Background()
    conc := newConcurrencyThrottle(2)

    project := "pokemon"
    asset := "pikachu"
    version := "red"

    t.Run("no failures", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }
    })

    t.Run("extra file", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        err = os.WriteFile(filepath.Join(reg, project, asset, version, "moves", "electric", "thunderwave"), []byte{}, 0644)
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "extra file") {
            t.Errorf("expected an error from extra file, got %v", err)
        }
    })

    t.Run("missing file", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        err = os.Remove(filepath.Join(reg, project, asset, version, "moves", "electric", "thunderbolt"))
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "cannot be found") {
            t.Errorf("expected an error from missing file, got %v", err)
        }
    })

    t.Run("wrong file size", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        // thunderbolt should be 90, but we replace it with 100 to add an extra character.
        err = os.WriteFile(filepath.Join(reg, project, asset, version, "moves", "electric", "thunderbolt"), []byte("100"), 0644)
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "incorrect size") {
            t.Errorf("expected an error from incorrect size, got %v", err)
        }
    })

    t.Run("wrong file checksum", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        // thunderbolt should be 90, but we replace it with 80 to change the MD5 sum without altering the size.
        err = os.WriteFile(filepath.Join(reg, project, asset, version, "moves", "electric", "thunderbolt"), []byte("80"), 0644)
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "incorrect MD5 checksum") {
            t.Errorf("expected an error from incorrect checksum, got %v", err)
        }
    })
}

func TestValidateDirectoryLocalLinks(t *testing.T) {
    src, err := setupSourceForValidateDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    ctx := context.Background()
    conc := newConcurrencyThrottle(2)

    err = os.Symlink("type", filepath.Join(src, "supertype"))
    if err != nil {
        t.Fatal(err)
    }

    project := "pokemon"
    asset := "pikachu"
    version := "red"

    t.Run("no failures", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }
    })

    t.Run("wrong link", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        v_path := filepath.Join(reg, project, asset, version)
        err = os.Remove(filepath.Join(v_path, "supertype"))
        if err != nil {
            t.Fatal(err)
        }
        err = os.Symlink(filepath.Join("moves", "electric", "thunderbolt"), filepath.Join(v_path, "supertype"))
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "non-ancestor, non-parent") {
            t.Errorf("expected an error from incorrect link, got %v", err)
        }
    })

    t.Run("wrong link again", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        v_path := filepath.Join(reg, project, asset, version)
        man, err := readManifest(v_path)
        if err != nil {
            t.Fatal(err)
        }

        man["supertype"].Link.Path = "moves/electric/thunderbolt" // modify it in the manifest to get past the RestoreLinkParent checks.
        err = dumpJson(filepath.Join(v_path, manifestFileName), &man)
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "mismatching link path") {
            t.Errorf("expected an error from incorrect link, got %v", err)
        }
    })

    t.Run("global link target", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        v_path := filepath.Join(reg, project, asset, version)
        err = os.Remove(filepath.Join(v_path, "supertype"))
        if err != nil {
            t.Fatal(err)
        }
        err = os.Symlink(filepath.Join(v_path, "type"), filepath.Join(v_path, "supertype"))
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "expected a relative path") {
            t.Errorf("expected an error from global link, got %v", err)
        }
    })

    t.Run("escaped link target", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        v_path := filepath.Join(reg, project, asset, version)
        err = os.Remove(filepath.Join(v_path, "supertype"))
        if err != nil {
            t.Fatal(err)
        }
        err = os.Symlink(filepath.Join("..", version, "type"), filepath.Join(v_path, "supertype"))
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "not local") {
            t.Errorf("expected an error from escaped link, got %v", err)
        }
    })
}

func TestValidateDirectoryRegistryLinks(t *testing.T) {
    src, err := setupSourceForValidateDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    ctx := context.Background()
    conc := newConcurrencyThrottle(2)

    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatal(err)
    }

    project := "pokemon"
    asset := "pikachu"
    err = transferDirectory(src, reg, project, asset, "red", ctx, &conc, transferDirectoryOptions{})
    if err != nil {
        t.Fatal(err)
    }
    err = os.WriteFile(filepath.Join(reg, project, asset, "red", summaryFileName), []byte("{}"), 0644)
    if err != nil {
        t.Fatalf("failed to create the summary file; %v", err)
    }

    // Control to check that everything is fine.
    err = os.Symlink(filepath.Join(reg, project, asset, "red", "type"), filepath.Join(src, "supertype"))
    if err != nil {
        t.Fatal(err)
    }

    err = transferDirectory(src, reg, project, asset, "blue", ctx, &conc, transferDirectoryOptions{})
    if err != nil {
        t.Fatal(err)
    }

    err = validateDirectory(reg, project, asset, "blue", ctx, &conc, validateDirectoryOptions{})
    if err != nil {
        t.Fatal(err)
    }

    // Retargeting the symlink with a global path. 
    v_path := filepath.Join(reg, project, asset, "blue")
    err = os.Remove(filepath.Join(v_path, "supertype"))
    if err != nil {
        t.Fatal(err)
    }
    err = os.Symlink(filepath.Join(reg, project, asset, "red", "type"), filepath.Join(v_path, "supertype"))
    if err != nil {
        t.Fatal(err)
    }

    err = validateDirectory(reg, project, asset, "blue", ctx, &conc, validateDirectoryOptions{})
    if err == nil || !strings.Contains(err.Error(), "expected a relative path") {
        t.Errorf("expected an error from global link, got %v", err)
    }

    // Retargeting the symlink with a relative path that traverses outside the registry.
    err = os.Remove(filepath.Join(v_path, "supertype"))
    if err != nil {
        t.Fatal(err)
    }
    err = os.Symlink(filepath.Join("..", "..", "..", "..", filepath.Base(reg), project, asset, "red", "type"), filepath.Join(v_path, "supertype"))
    if err != nil {
        t.Fatal(err)
    }

    err = validateDirectory(reg, project, asset, "blue", ctx, &conc, validateDirectoryOptions{})
    if err == nil || !strings.Contains(err.Error(), "not local") {
        t.Errorf("expected an error from global link, got %v", err)
    }
}

func TestValidateDirectoryAncestors(t *testing.T) {
    src, err := setupSourceForValidateDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    ctx := context.Background()
    conc := newConcurrencyThrottle(2)

    err = os.Symlink("type", filepath.Join(src, "supertype"))
    if err != nil {
        t.Fatal(err)
    }

    err = os.Symlink("supertype", filepath.Join(src, "megatype"))
    if err != nil {
        t.Fatal(err)
    }

    err = os.Symlink("megatype", filepath.Join(src, "ultratype"))
    if err != nil {
        t.Fatal(err)
    }

    project := "pokemon"
    asset := "pikachu"
    version := "yellow"

    t.Run("no failures", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }
    })

    t.Run("missing ancestor", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        v_path := filepath.Join(reg, project, asset, version)
        man, err := readManifest(v_path)
        if err != nil {
            t.Fatal(err)
        }

        man["megatype"].Link.Ancestor = nil
        err = dumpJson(filepath.Join(v_path, manifestFileName), &man)
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "expected link ancestor") {
            t.Errorf("expected an error from a missing ancestor, got %v", err)
        }
    })

    t.Run("unexpected ancestor", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        v_path := filepath.Join(reg, project, asset, version)
        man, err := readManifest(v_path)
        if err != nil {
            t.Fatal(err)
        }

        man["supertype"].Link.Ancestor = &linkMetadata{}
        err = dumpJson(filepath.Join(v_path, manifestFileName), &man)
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "unexpected link ancestor") {
            t.Errorf("expected an error from an extra ancestor, got %v", err)
        }
    })
}

