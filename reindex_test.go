package main

import (
    "testing"
    "os"
    "path/filepath"
    "fmt"
    "os/user"
    "strings"
    "context"
    "io/fs"
    "sort"
)

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

func TestReindexDirectorySimple(t *testing.T) {
    project := "pokemon"
    asset := "pikachu"
    version := "red"

    src, err := setupSourceForWalkDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    ctx := context.Background()
    conc := newConcurrencyThrottle(2)

    // Setting up the transfer.
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
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

    err = reindexDirectory(reg, project, asset, version, ctx, &conc, reindexDirectoryOptions{})
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

    src, err := setupSourceForWalkDirectoryTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Setting up the transfer.
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()
    conc := newConcurrencyThrottle(2)

    err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
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
    err = reindexDirectory(reg, project, asset, version, ctx, &conc, reindexDirectoryOptions{})
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

/**********************************************
 **********************************************/

func TestReindexDirectoryRegistryLinks(t *testing.T) {
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
    simulate_versions := func(project, asset string){
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

    t.Run("no ancestors", func(t *testing.T) {
        project := "pokemon"
        asset := "pikachu"
        simulate_versions(project, asset)

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

        err = reindexDirectory(reg, project, asset, version, ctx, &conc, reindexDirectoryOptions{})
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

        // Confirming that, indeed, there are no ancestors.
        man, err := readManifest(v_path)
        if err != nil {
            t.Fatal(err)
        }
        for mpath, mentry := range man {
            if mentry.Link != nil {
                if mentry.Link.Ancestor != nil {
                    t.Errorf("unexpected ancestor for %q; %v", mpath, *(mentry.Link.Ancestor))
                }
            }
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
    })

    t.Run("preserves ancestors", func(t *testing.T) {
        project := "pokemon"
        asset := "raichu"
        simulate_versions(project, asset)

        version := "green"
        v_path := filepath.Join(reg, project, asset, version)
        prior, err := loadDirectoryContents(v_path)
        if err != nil {
            t.Fatalf("failed to load directory contents; %v", err)
        }

        // Checking that reindexing preserves ancestral information if ..links are present, so we only remove the manifest.
        err = os.Remove(filepath.Join(v_path, manifestFileName))
        if err != nil {
            t.Fatal(err)
        }

        err = reindexDirectory(reg, project, asset, version, ctx, &conc, reindexDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to reindex directory; %v", err)
        }

        man, err := readManifest(v_path)
        if err != nil {
            t.Fatal(err)
        }
        for mpath, mentry := range man {
            if mentry.Link == nil {
                t.Errorf("expected link for %q", mpath)
            } else if mentry.Link.Ancestor == nil {
                t.Errorf("expected ancestor for %q; %v", mpath, *(mentry.Link))
            }
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

    t.Run("wipes ancestors", func(t *testing.T) {
        project := "pokemon"
        asset := "pichu"
        simulate_versions(project, asset)

        version := "green"
        v_path := filepath.Join(reg, project, asset, version)
        prior, err := readManifest(v_path)
        if err != nil {
            t.Fatal(err)
        }

        // Now reindexing after we purge all the '..xxx' files, including the links.
        err = stripDoubleDotFiles(v_path)
        if err != nil {
            t.Fatalf("failed to strip all double dots; %v", err)
        }
        err = os.WriteFile(filepath.Join(v_path, summaryFileName), []byte("{}"), 0644) // mocking this up for a valid comparison.
        if err != nil {
            t.Fatalf("failed to create the latest file; %v", err)
        }

        err = reindexDirectory(reg, project, asset, version, ctx, &conc, reindexDirectoryOptions{})
        if err != nil {
            t.Fatalf("failed to reindex directory; %v", err)
        }

        // All ancestral information is now lost.
        man, err := readManifest(v_path)
        if err != nil {
            t.Fatal(err)
        }
        for mpath, mentry := range man {
            if mentry.Link == nil {
                t.Errorf("expected link for %q", mpath)
            } else if mentry.Link.Ancestor != nil {
                t.Errorf("unexpected ancestor for %q; %v", mpath, *(mentry.Link))
            } else {
                previous, ok := prior[mpath]
                if !ok {
                    t.Errorf("could not find %q before reindexing", mpath)
                } else if (mentry.Link.Project != previous.Link.Ancestor.Project || 
                    mentry.Link.Asset != previous.Link.Ancestor.Asset || 
                    mentry.Link.Version != previous.Link.Ancestor.Version || 
                    mentry.Link.Path != previous.Link.Ancestor.Path) {
                    t.Errorf("unexpected link target for %q after reindexing; %v", mpath, *(mentry.Link))
                }
            }
        }
    })
}

func TestReindexDirectoryRegistryLinkFailures(t *testing.T) {
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

    t.Run("external files", func(t *testing.T) {
        project := "pokemon"
        asset := "lugia"
        version := "silver"
        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
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

        err = reindexDirectory(reg, project, asset, version, ctx, &conc, reindexDirectoryOptions{})
        if err == nil || !strings.Contains(err.Error(), "outside the registry") {
            t.Errorf("expected reindexing failure for files outside the registry; %v", err)
        }
    })

    // All other failures are handled by resolveRegistrySymlink and are common to
    // both transfer and reindex functions, so we won't test them again here.
}

/**********************************************
 **********************************************/

func TestReindexDirectoryLocalLinks(t *testing.T) {
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

    err = os.Symlink(filepath.Join("..", "type2"), filepath.Join(src, "evolution", "foo")) // relative symlink to another symlink.
    if err != nil {
        t.Fatalf("failed to create a symlink for 'evolution/foo'; %v", err)
    }

    t.Run("simple", func(t *testing.T) {
        project := "POKEMON"
        asset := "PIKAPIKA"
        version := "GOLD"

        err := transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
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

        err = reindexDirectory(reg, project, asset, version, ctx, &conc, reindexDirectoryOptions{})
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
        err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
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

        err = reindexDirectory(reg, project, asset, version, ctx, &conc, reindexDirectoryOptions{})
        if err != nil {
            t.Fatal(err)
        }

        recovered, err := loadDirectoryContents(v_path)
        if err != nil {
            t.Fatalf("failed to load directory contents; %v", err)
        }

        // This time, the ancestral information of evolution/foo->type->type2 is preserved.
        err = compareDirectoryContents(prior, recovered)
        if err != nil {
            t.Error(err)
        }
    })

    // All failures are handled by resolveLocalSymlink and are common to
    // both transfer and reindex functions, so we won't test them again here.
}

/**********************************************
 **********************************************/

func TestReindexDirectoryLinkWhitelist(t *testing.T) {
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

    // Mocking up a directory structure. 
    project := "pokemon"
    asset := "lugia"
    version := "silver"
    err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
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

    err = reindexDirectory(reg, project, asset, version, ctx, &conc, reindexDirectoryOptions{ LinkWhitelist: []string{ filepath.Dir(other_name) } })
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

func TestReindexDirectoryRestoreLinkParentFailure(t *testing.T) {
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

    // Mocking up a directory structure.
    project := "pokemon"
    asset := "charmander"
    version := "fire-red"
    err = transferDirectory(src, reg, project, asset, version, ctx, &conc, transferDirectoryOptions{})
    if err != nil {
        t.Fatalf("failed to perform the transfer; %v", err)
    }

    // Now, we change the link so that we're targeting the middle ancestor rather than the direct parent.
    change_path := filepath.Join(reg, project, asset, version, "ultratype")
    target, err := os.Readlink(change_path)
    if err != nil {
        t.Fatal(err)
    }
    if target != "type" {
        t.Error("expected 'ultratype' to target 'type'")
    }

    err = os.Remove(change_path)
    if err != nil {
        t.Fatal(err)
    }
    err = os.Symlink("supertype", change_path)
    if err != nil {
        t.Fatal(err)
    }

    err = reindexDirectory(reg, project, asset, version, ctx, &conc, reindexDirectoryOptions{})
    if err == nil || !strings.Contains(err.Error(), "non-ancestor, non-parent") {
        t.Error("should have thrown an error if the link does not target the ancestor or parent")
    }

    // As a control, we change it again to target just the parent, in which case all should be well.
    err = os.Remove(change_path)
    if err != nil {
        t.Fatal(err)
    }
    err = os.Symlink("megatype", change_path)
    if err != nil {
        t.Fatal(err)
    }

    err = reindexDirectory(reg, project, asset, version, ctx, &conc, reindexDirectoryOptions{})
    if err != nil {
        t.Fatal(err)
    }

    target, err = os.Readlink(change_path)
    if err != nil {
        t.Fatal(err)
    }
    if target != "type" {
        t.Error("expected 'ultratype' to target 'type' after reindexing")
    }
}

/**********************************************
 **********************************************/

func setupDirectoryForReindexHandlerTest(globals *globalConfiguration, project, asset, version string) (string, error) {
    self, err := user.Current()
    if err != nil {
        return "", fmt.Errorf("failed to determine the current user; %w", err)
    }

    project_dir := filepath.Join(globals.Registry, project)
    err = createProject(project_dir, nil, self.Username)
    if err != nil {
        return "", err
    }

    asset_dir := filepath.Join(project_dir, asset)
    dir := filepath.Join(asset_dir, version)
    err = os.MkdirAll(dir, 0755)
    if err != nil {
        return "", err
    }

    err = os.WriteFile(filepath.Join(dir, summaryFileName), []byte(`{ 
    "upload_user_id": "aaron",
    "upload_start": "2025-01-26T11:28:10Z",
    "upload_finish": "2025-01-26T11:28:20Z"
}`), 0644)

    err = os.WriteFile(filepath.Join(dir, "evolution"), []byte("haunter"), 0644)
    if err != nil {
        return "", err
    }

    err = os.WriteFile(filepath.Join(dir, "moves"), []byte("lick,confuse_ray,shadow_ball,dream_eater"), 0644)
    if err != nil {
        return "", err
    }

    return self.Username, nil
}

func TestReindexHandlerSimple(t *testing.T) {
    project := "original_series"
    asset := "gastly"
    version := "lavender"

    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg, 2)

    self_user, err := setupDirectoryForReindexHandlerTest(&globals, project, asset, version)
    if err != nil {
        t.Fatalf("failed to set up project directory; %v", err)
    }
    globals.Administrators = append(globals.Administrators, self_user)

    ctx := context.Background()

    // Performing the request.
    req_string := fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version)
    reqname, err := dumpRequest("reindex", req_string)
    if err != nil {
        t.Fatalf("failed to create reindex request; %v", err)
    }

    err = reindexHandler(reqname, &globals, ctx)
    if err != nil {
        t.Fatalf("failed to perform the reindexing; %v", err)
    }

    // Checking a few manifest entries and files.
    asset_dir := filepath.Join(reg, project, asset)
    destination := filepath.Join(asset_dir, version)
    man, err := readManifest(destination)
    if err != nil {
        t.Fatalf("failed to read the manifest; %v", err)
    }
    info, ok := man["evolution"]
    if !ok || int(info.Size) != len("haunter") || info.Link != nil {
        t.Fatal("unexpected manifest entry for 'evolution'")
    }
    err = verifyFileContents(filepath.Join(destination, "moves"), "lick,confuse_ray,shadow_ball,dream_eater")
    if err != nil {
        t.Fatalf("could not verify 'moves'; %v", err)
    }

    // Checking that the logs have something in them.
    logs, err := readAllLogs(reg)
    if err != nil {
        t.Fatalf("failed to read the logs; %v", err)
    }
    if len(logs) != 1 {
        t.Fatalf("expected exactly one entry in the log directory")
    }
    if logs[0].Type != "reindex-version" || 
        logs[0].Project == nil || *(logs[0].Project) != project || 
        logs[0].Asset == nil || *(logs[0].Asset) != asset || 
        logs[0].Version == nil || *(logs[0].Version) != version ||
        logs[0].Latest == nil || *(logs[0].Latest) {
        t.Fatalf("unexpected contents for first log in %q", reg)
    }
}

func TestReindexHandlerLatest(t *testing.T) {
    project := "original_series"
    asset := "gastly"
    version := "lavender"

    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    globals := newGlobalConfiguration(reg, 2)

    self_user, err := setupDirectoryForReindexHandlerTest(&globals, project, asset, version)
    if err != nil {
        t.Fatalf("failed to set up project directory; %v", err)
    }
    globals.Administrators = append(globals.Administrators, self_user)

    asset_dir := filepath.Join(reg, project, asset)
    err = os.WriteFile(filepath.Join(asset_dir, latestFileName), []byte(fmt.Sprintf(`{ "version": "%s" }`, version)), 0644)
    if err != nil {
        t.Fatal(err)
    }

    ctx := context.Background()

    // Performing the request.
    req_string := fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version)
    reqname, err := dumpRequest("reindex", req_string)
    if err != nil {
        t.Fatalf("failed to create reindex request; %v", err)
    }

    err = reindexHandler(reqname, &globals, ctx)
    if err != nil {
        t.Fatalf("failed to perform the reindexing; %v", err)
    }

    logs, err := readAllLogs(reg)
    if err != nil {
        t.Fatalf("failed to read the logs; %v", err)
    }
    if len(logs) != 1 {
        t.Fatalf("expected exactly two entries in the log directory")
    }
    if logs[0].Type != "reindex-version" || 
        logs[0].Latest == nil || !*(logs[0].Latest) { // this time, we did reindex the latest one.
        t.Fatalf("unexpected contents for second log in %q", reg)
    }
}

func TestReindexHandlerProbation(t *testing.T) {
    project := "original_series"
    asset := "gastly"
    version := "lavender"

    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    globals := newGlobalConfiguration(reg, 2)

    self_user, err := setupDirectoryForReindexHandlerTest(&globals, project, asset, version)
    if err != nil {
        t.Fatalf("failed to set up project directory; %v", err)
    }
    globals.Administrators = append(globals.Administrators, self_user)

    // Set it to be on probation.
    err = os.WriteFile(filepath.Join(globals.Registry, project, asset, version, summaryFileName), []byte(`{ 
    "upload_user_id": "aaron",
    "upload_start": "2025-01-26T11:28:10Z",
    "upload_finish": "2025-01-26T11:28:20Z",
    "on_probation": true
}`), 0644)

    // Performing the request.
    ctx := context.Background()

    req_string := fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version)
    reqname, err := dumpRequest("reindex", req_string)
    if err != nil {
        t.Fatalf("failed to create reindex request; %v", err)
    }

    err = reindexHandler(reqname, &globals, ctx)
    if err != nil {
        t.Fatalf("failed to perform the reindexing; %v", err)
    }

    // Manifests are generated but not the log file.
    destination := filepath.Join(reg, project, asset, version)
    _, err = readManifest(destination)
    if err != nil {
        t.Fatalf("failed to read the manifest; %v", err)
    }

    logs, err := readAllLogs(reg)
    if err != nil {
        t.Fatalf("failed to read the logs; %v", err)
    }
    if len(logs) != 0 {
        t.Fatalf("expected no entries in the log directory")
    }
}

func TestReindexHandlerSimpleFailures(t *testing.T) {
    project := "test"
    asset := "gastly"
    version := "lavender"

    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    globals := newGlobalConfiguration(reg, 2)

    self_user, err := setupDirectoryForReindexHandlerTest(&globals, project, asset, version)
    if err != nil {
        t.Fatalf("failed to set up project directory; %v", err)
    }
    globals.Administrators = append(globals.Administrators, self_user)

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

func TestReindexHandlerUnauthorized(t *testing.T) {
    project := "test"
    asset := "gastly"
    version := "lavender"

    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg, 2)
    ctx := context.Background()

    _, err = setupDirectoryForReindexHandlerTest(&globals, project, asset, version)
    if err != nil {
        t.Fatalf("failed to set up project directory; %v", err)
    }

    req_string := fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version)
    reqname, err := dumpRequest("reindex", req_string)
    if err != nil {
        t.Fatalf("failed to create reindex request; %v", err)
    }

    err = reindexHandler(reqname, &globals, ctx)
    if err == nil || !strings.Contains(err.Error(), "not authorized") {
        t.Fatalf("failed to reject reindex from non-authorized user")
    }
}
