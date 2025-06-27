package main

import (
    "testing"
    "os"
    "path/filepath"
    "strings"
    "context"
    "fmt"
    "os/user"
)

func TestValidateDirectorySimple(t *testing.T) {
    src, err := setupSourceForWalkDirectoryTest()
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
    src, err := setupSourceForWalkDirectoryTest()
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
    src, err := setupSourceForWalkDirectoryTest()
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
    src, err := setupSourceForWalkDirectoryTest()
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

func TestValidateDirectoryLinkfiles(t *testing.T) {
    src, err := setupSourceForWalkDirectoryTest()
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

    err = os.Symlink(filepath.Join("..", "normal", "quick_attack"), filepath.Join(src, "moves", "electric", "thunderwave"))
    if err != nil {
        t.Fatal(err)
    }

    project := "pokemon"
    asset := "pikachu"
    version := "yellow"

    t.Run("missing linkfile", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        err = os.Remove(filepath.Join(reg, project, asset, version, "moves", "electric", linksFileName))
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "expected a linkfile") {
            t.Errorf("expected an error from a missing linkfile, got %v", err)
        }
    })

    t.Run("extra linkfile", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        err = os.WriteFile(filepath.Join(reg, project, asset, version, "moves", linksFileName), []byte("{}"), 0644)
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "extra linkfile") {
            t.Errorf("expected an error from an extra linkfile, got %v", err)
        }
    })

    t.Run("missing path in linkfile", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        err = os.WriteFile(filepath.Join(reg, project, asset, version, "moves", "electric", linksFileName), []byte("{}"), 0644)
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "missing path") {
            t.Errorf("expected an error from a missing path in the linkfile, got %v", err)
        }
    })

    t.Run("wrong information in linkfile", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        lpath := filepath.Join(reg, project, asset, version, linksFileName)
        links, err := readLinkfile(lpath)
        if err != nil {
            t.Fatal(err)
        }

        links["megatype"].Path = "type" // go straight to the ancestor this time.
        links["megatype"].Ancestor = nil
        err = dumpJson(lpath, &links)
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "mismatching link information") {
            t.Errorf("expected an error from mismatching linkfile information, got %v", err)
        }
    })

    t.Run("extra path in linkfile", func(t *testing.T) {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        lpath := filepath.Join(reg, project, asset, version, linksFileName)
        links, err := readLinkfile(lpath)
        if err != nil {
            t.Fatal(err)
        }

        links["whee"] = &linkMetadata{}
        err = dumpJson(lpath, &links)
        if err != nil {
            t.Fatal(err)
        }

        err = validateDirectory(reg, project, asset, version, ctx, &conc, validateDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "extra path") {
            t.Errorf("expected an error from extra path in the linkfile, got %v", err)
        }
    })
}

/**********************************************
 **********************************************/

func setupDirectoryForValidateHandlerTest(registry, project, asset, version string) error {
    project_dir := filepath.Join(registry, project)

    asset_dir := filepath.Join(project_dir, asset)
    dir := filepath.Join(asset_dir, version)
    err := os.MkdirAll(dir, 0755)
    if err != nil {
        return err
    }

    err = os.WriteFile(filepath.Join(dir, summaryFileName), []byte(`{ 
    "upload_user_id": "luna",
    "upload_start": "2025-05-01T02:23:32Z",
    "upload_finish": "2025-05-01T04:45:09Z"
}`), 0644)

    err = os.WriteFile(filepath.Join(dir, "foo"), []byte("bar"), 0644)
    if err != nil {
        return err
    }

    conc := newConcurrencyThrottle(1)
    err = reindexDirectory(registry, project, asset, version, context.Background(), &conc, reindexDirectoryOptions{})
    return nil
}

func TestValidateHandlerSimple(t *testing.T) {
    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg, 2)

    self, err := user.Current()
    if err != nil {
        t.Fatal(err)
    }
    globals.Administrators = append(globals.Administrators, self.Username)

    ctx := context.Background()

    project := "horizons"
    err = createProject(filepath.Join(reg, project), nil, "tin")
    if err != nil {
        t.Fatal(err)
    }

    t.Run("success", func(t *testing.T) {
        asset := "chikorita"
        version := "silver"

        err := setupDirectoryForValidateHandlerTest(reg, project, asset, version)
        if err != nil {
            t.Fatalf("failed to set up project directory; %v", err)
        }

        req_string := fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version)
        reqname, err := dumpRequest("validate", req_string)
        if err != nil {
            t.Fatalf("failed to create reindex request; %v", err)
        }

        err = validateHandler(reqname, &globals, ctx)
        if err != nil {
            t.Fatal(err)
        }
    })

    t.Run("extra file", func(t *testing.T) {
        asset := "totodile"
        version := "gold"

        err := setupDirectoryForValidateHandlerTest(reg, project, asset, version)
        if err != nil {
            t.Fatalf("failed to set up project directory; %v", err)
        }

        // Injecting an extra file to check that we indeed throw an error.
        err = os.WriteFile(filepath.Join(reg, project, asset, version, "whee"), []byte("stuff"), 0644)
        if err != nil {
            t.Fatal(err)
        }

        req_string := fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version)
        reqname, err := dumpRequest("validate", req_string)
        if err != nil {
            t.Fatalf("failed to create reindex request; %v", err)
        }

        err = validateHandler(reqname, &globals, ctx)
        if err == nil || !strings.Contains(err.Error(), "extra file") {
            t.Errorf("expected a validation error from an extra file, got %v", err)
        }
    })

    t.Run("invalid summary", func(t *testing.T) {
        asset := "cyndaquil"
        version := "crystal"

        err := setupDirectoryForValidateHandlerTest(reg, project, asset, version)
        if err != nil {
            t.Fatalf("failed to set up project directory; %v", err)
        }

        req_string := fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version)
        reqname, err := dumpRequest("validate", req_string)
        if err != nil {
            t.Fatalf("failed to create reindex request; %v", err)
        }

        err = os.WriteFile(filepath.Join(reg, project, asset, version, summaryFileName), []byte("{}"), 0644)
        if err != nil {
            t.Fatal(err)
        }
        err = validateHandler(reqname, &globals, ctx)
        if err == nil || !strings.Contains(err.Error(), "invalid 'upload_user_id'") {
            t.Errorf("expected a validation error from invalid summary, got %v", err)
        }

        err = os.WriteFile(filepath.Join(reg, project, asset, version, summaryFileName), []byte(`{ "upload_user_id": "aaron" }`), 0644)
        if err != nil {
            t.Fatal(err)
        }
        err = validateHandler(reqname, &globals, ctx)
        if err == nil || !strings.Contains(err.Error(), "could not parse 'upload_start'") {
            t.Errorf("expected a validation error from invalid summary, got %v", err)
        }

        err = os.WriteFile(filepath.Join(reg, project, asset, version, summaryFileName), []byte(`{ "upload_user_id": "aaron", "upload_start": "2022-12-22T22:22:22Z" }`), 0644)
        if err != nil {
            t.Fatal(err)
        }
        err = validateHandler(reqname, &globals, ctx)
        if err == nil || !strings.Contains(err.Error(), "could not parse 'upload_finish'") {
            t.Errorf("expected a validation error from invalid summary, got %v", err)
        }
    })
}

func TestValidateHandlerPreflight(t *testing.T) {
    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    globals := newGlobalConfiguration(reg, 2)
    ctx := context.Background()

    t.Run("bad project", func(t *testing.T) {
        reqname, err := dumpRequest("reindex", `{ "asset": "foo", "version": "bar" }`)
        if err != nil {
            t.Fatalf("failed to create reindex request; %v", err)
        }
        err = reindexHandler(reqname, &globals, ctx)
        if err == nil || !strings.Contains(err.Error(), "expected a 'project'") {
            t.Fatal("configuration should fail for missing project")
        }

        reqname, err = dumpRequest("reindex", `{ "project": "bad/name", "asset": "foo", "version": "bar" }`)
        if err != nil {
            t.Fatalf("failed to create reindex request; %v", err)
        }
        err = reindexHandler(reqname, &globals, ctx)
        if err == nil || !strings.Contains(err.Error(), "invalid project name") {
            t.Fatal("configuration should fail for invalid project name")
        }
    })

    t.Run("bad asset", func(t *testing.T) {
        reqname, err := dumpRequest("reindex", `{ "project": "foo", "version": "bar" }`)
        if err != nil {
            t.Fatalf("failed to create reindex request; %v", err)
        }
        err = reindexHandler(reqname, &globals, ctx)
        if err == nil || !strings.Contains(err.Error(), "expected an 'asset'") {
            t.Fatal("configuration should fail for missing asset")
        }

        reqname, err = dumpRequest("reindex", `{ "project": "foo", "asset": "..bar", "version": "bar" }`)
        if err != nil {
            t.Fatalf("failed to create reindex request; %v", err)
        }
        err = reindexHandler(reqname, &globals, ctx)
        if err == nil || !strings.Contains(err.Error(), "invalid asset name") {
            t.Fatal("configuration should fail for invalid asset name")
        }
    })

    t.Run("bad version", func(t *testing.T) {
        reqname, err := dumpRequest("reindex", `{ "project": "foo", "asset": "bar" }`) 
        if err != nil {
            t.Fatalf("failed to create reindex request; %v", err)
        }
        err = reindexHandler(reqname, &globals, ctx)
        if err == nil || !strings.Contains(err.Error(), "expected a 'version'") {
            t.Fatal("configuration should fail for missing version")
        }

        reqname, err = dumpRequest("reindex", `{ "project": "foo", "asset": "bar", "version": "" }`)
        if err != nil {
            t.Fatalf("failed to create reindex request; %v", err)
        }
        err = reindexHandler(reqname, &globals, ctx)
        if err == nil || !strings.Contains(err.Error(), "invalid version name") {
            t.Fatal("configuration should fail for invalid version name")
        }
    })
}

func TestValidateHandlerUnauthorized(t *testing.T) {
    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg, 2)
    ctx := context.Background()

    req_string := `{ "project": "foo", "asset": "bar", "version": "whee" }`
    reqname, err := dumpRequest("reindex", req_string)
    if err != nil {
        t.Fatalf("failed to create reindex request; %v", err)
    }

    err = reindexHandler(reqname, &globals, ctx)
    if err == nil || !strings.Contains(err.Error(), "not authorized") {
        t.Fatalf("failed to reject reindex from non-authorized user")
    }
}
