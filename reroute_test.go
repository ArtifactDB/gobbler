package main

import (
    "testing"
    "os"
    "path/filepath"
    "fmt"
    "errors"
    "encoding/json"
    "strings"
)

func mockRegistryForReroute(registry, project, asset string) error {
    src, err := os.MkdirTemp("", "")
    if err != nil {
        return fmt.Errorf("failed to create the temporary directory; %w", err)
    }

    // First import.
    {
        err := os.WriteFile(filepath.Join(src, "akari"), []byte("mizunashi"), 0644)
        if err != nil {
            return err
        }

        err = os.WriteFile(filepath.Join(src, "alicia"), []byte("florence"), 0644)
        if err != nil {
            return err
        }

        err = transferDirectory(src, registry, project, asset, "animation", []string{})
        if err != nil {
            return err
        }

        err = dumpJson(filepath.Join(registry, project, asset, latestFileName), map[string]string{ "version": "animation" })
        if err != nil {
            return err
        }
    }

    // Second import.
    {
        opath := filepath.Join(src, "orange_planet")
        err := os.Mkdir(opath, 0755)
        if err != nil {
            return err
        }

        err = os.WriteFile(filepath.Join(opath, "alice"), []byte("carroll"), 0644)
        if err != nil {
            return err
        }

        err = os.WriteFile(filepath.Join(opath, "athena"), []byte("glory"), 0644)
        if err != nil {
            return err
        }

        err = transferDirectory(src, registry, project, asset, "natural", []string{})
        if err != nil {
            return err
        }

        // Confirm that we have links.
        if _, err := os.Stat(filepath.Join(registry, project, asset, "natural", linksFileName)); err != nil {
            return err
        }

        err = dumpJson(filepath.Join(registry, project, asset, latestFileName), map[string]string{ "version": "natural" })
        if err != nil {
            return err
        }
    }

    // Third import.
    {
        hpath := filepath.Join(src, "himeya")
        err = os.Mkdir(hpath, 0755)
        if err != nil {
            return err
        }

        err = os.WriteFile(filepath.Join(hpath, "aika"), []byte("granzchesta"), 0644)
        if err != nil {
            return err
        }

        err = os.WriteFile(filepath.Join(hpath, "akira"), []byte("ferrari"), 0644)
        if err != nil {
            return err
        }

        err = transferDirectory(src, registry, project, asset, "origination", []string{})
        if err != nil {
            return err
        }

        // Confirm that we have links.
        if _, err := os.Stat(filepath.Join(registry, project, asset, "origination", linksFileName)); err != nil {
            return err
        }
        if _, err := os.Stat(filepath.Join(registry, project, asset, "origination", "orange_planet", linksFileName)); err != nil {
            return err
        }

        err = dumpJson(filepath.Join(registry, project, asset, latestFileName), map[string]string{ "version": "origination" })
        if err != nil {
            return err
        }
    }

    // Fourth import.
    {
        err := os.WriteFile(filepath.Join(src, "ai"), []byte("aino"), 0644)
        if err != nil {
            return err
        }

        err = transferDirectory(src, registry, project, asset, "avvenire", []string{})
        if err != nil {
            return err
        }

        // Confirm that we have links.
        if _, err := os.Stat(filepath.Join(registry, project, asset, "origination", linksFileName)); err != nil {
            return err
        }

        err = dumpJson(filepath.Join(registry, project, asset, latestFileName), map[string]string{ "version": "avvinere" })
        if err != nil {
            return err
        }
    }

    // Setting the total usage.
    total_usage := usageMetadata{
        Total: int64(
            len("mizunashi") + 
            len("florence") + 
            len("carroll") + 
            len("glory") + 
            len("granzchesta") + 
            len("ferrari") + 
            len("aino"),
        ),
    }
    err = dumpJson(filepath.Join(registry, project, usageFileName), &total_usage)
    if err != nil {
        return err
    }

    return nil
}

func TestListToBeDeletedVersions(t *testing.T) {
    registry, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the temporary directory; %v", err)
    }

    project1 := "ARIA" 
    asset1 := "anime"
    err = mockRegistryForReroute(registry, project1, asset1)
    if err != nil {
        t.Fatal(err)
    }

    asset2 := "foobar"
    err = mockRegistryForReroute(registry, project1, asset2)
    if err != nil {
        t.Fatal(err)
    }

    project2 := "arietta"
    err = mockRegistryForReroute(registry, project2, asset1)
    if err != nil {
        t.Fatal(err)
    }

    t.Run("by project", func(t *testing.T) {
        to_delete_versions, err := listToBeDeletedVersions(registry, []deleteTask{ deleteTask{ Project: project1 } })
        if err != nil {
            t.Fatal(err)
        }
        if len(to_delete_versions) != 8 ||
            !to_delete_versions["ARIA/anime/animation"] ||
            !to_delete_versions["ARIA/anime/natural"] ||
            !to_delete_versions["ARIA/anime/origination"] ||
            !to_delete_versions["ARIA/anime/avvenire"] ||
            !to_delete_versions["ARIA/foobar/animation"] ||
            !to_delete_versions["ARIA/foobar/natural"] ||
            !to_delete_versions["ARIA/foobar/origination"] ||
            !to_delete_versions["ARIA/foobar/avvenire"] {
            t.Errorf("expected more things to be deleted; %v", to_delete_versions)
        }
    })

    t.Run("by asset", func(t *testing.T) {
        to_delete_versions, err := listToBeDeletedVersions(registry, []deleteTask{ deleteTask{ Project: project1, Asset: &asset1 } })
        if err != nil {
            t.Fatal(err)
        }
        if len(to_delete_versions) != 4 ||
            !to_delete_versions["ARIA/anime/animation"] ||
            !to_delete_versions["ARIA/anime/natural"] ||
            !to_delete_versions["ARIA/anime/origination"] ||
            !to_delete_versions["ARIA/anime/avvenire"] {
            t.Errorf("expected more things to be deleted; %v", to_delete_versions)
        }
    })

    version1 := "animation"
    version2 := "natural"
    t.Run("by version", func(t *testing.T) {
        to_delete_versions, err := listToBeDeletedVersions(registry, []deleteTask{ deleteTask{ Project: project1, Asset: &asset1, Version: &version2 } })
        if err != nil {
            t.Fatal(err)
        }
        if len(to_delete_versions) != 1 || !to_delete_versions["ARIA/anime/natural"] {
            t.Errorf("expected more things to be deleted; %v", to_delete_versions)
        }
    })

    t.Run("multiple", func(t *testing.T) {
        to_delete_versions, err := listToBeDeletedVersions(registry, []deleteTask{ 
            deleteTask{ Project: project1, Asset: &asset1, Version: &version2 },
            deleteTask{ Project: "arietta", Asset: &asset1, Version: &version1 },
        })
        if err != nil {
            t.Fatal(err)
        }
        if len(to_delete_versions) != 2 || !to_delete_versions["ARIA/anime/natural"] || !to_delete_versions["arietta/anime/animation"] {
            t.Errorf("expected more things to be deleted; %v", to_delete_versions)
        }
    })
}

func TestListToBeDeletedFiles(t *testing.T) {
    registry, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the temporary directory; %v", err)
    }

    project := "ARIA" 
    asset := "anime"
    err = mockRegistryForReroute(registry, project, asset)
    if err != nil {
        t.Fatal(err)
    }

    to_delete_files, err := listToBeDeletedFiles(registry, map[string]bool{ "ARIA/anime/animation": true, "ARIA/anime/origination": true })
    if err != nil {
        t.Fatal(err)
    }

    if len(to_delete_files) != 8 ||
        !to_delete_files["ARIA/anime/animation/akari"] ||
        !to_delete_files["ARIA/anime/animation/alicia"] ||
        !to_delete_files["ARIA/anime/origination/akari"] ||
        !to_delete_files["ARIA/anime/origination/alicia"] ||
        !to_delete_files["ARIA/anime/origination/orange_planet/alice"] ||
        !to_delete_files["ARIA/anime/origination/orange_planet/athena"] ||
        !to_delete_files["ARIA/anime/origination/himeya/aika"] ||
        !to_delete_files["ARIA/anime/origination/himeya/akira"] {
        t.Errorf("unexpected files to be deleted; %v", to_delete_files)
    }
}

func readLinks(path string) (map[string]linkMetadata, error) {
    link_details := map[string]linkMetadata{}
    contents, err := os.ReadFile(filepath.Join(path, linksFileName))
    if err != nil {
        return nil, err
    }

    err = json.Unmarshal(contents, &link_details)
    if err != nil {
        return nil, err
    }

    return link_details, nil
}

func invertChangelog(changes []rerouteAction) map[string]rerouteAction {
    output := map[string]rerouteAction{}
    for _, x := range changes {
        output[x.Path] = x
    }
    return output
}

func TestRerouteLinksForVersion(t *testing.T) {
    t.Run("delete ancestor, reroute child", func(t *testing.T) {
        registry, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        project := "ARIA" 
        asset := "anime"
        err = mockRegistryForReroute(registry, project, asset)
        if err != nil {
            t.Fatal(err)
        }

        to_delete_files, err := listToBeDeletedFiles(registry, map[string]bool{ "ARIA/anime/animation": true })
        if err != nil {
            t.Fatal(err)
        }

        version_dir := filepath.Join(project, asset, "natural")
        proposed, err := proposeLinkReroutes(registry, to_delete_files, version_dir)
        if err != nil {
            t.Fatal(err)
        }

        // Check that changes affect 'akari' and 'alicia' in all 3 subsequent versions.
        inverted_changes := invertChangelog(proposed.Actions)
        if len(inverted_changes) != 2 ||
            !inverted_changes["ARIA/anime/natural/akari"].Copy || 
            !inverted_changes["ARIA/anime/natural/alicia"].Copy {
            t.Errorf("unexpected changelog; %v", proposed.Actions)
        }

        err = executeLinkReroutes(registry, version_dir, proposed)
        if err != nil {
            t.Fatal(err)
        }

        full_vpath := filepath.Join(registry, version_dir)
        man, err := readManifest(full_vpath)
        if err != nil {
            t.Fatal(err)
        }

        // Check that a copy is made.
        {
            entry, found := man["akari"]
            if !found || entry.Link != nil {
                t.Errorf("unexpected rerouting; %v", entry)
            }

            apath := filepath.Join(registry, version_dir, "akari")
            if contents, err := os.ReadFile(apath); err != nil || string(contents) != "mizunashi" {
                t.Errorf("unexpected contents for akari; %v", string(contents))
            }
            if info, err := os.Lstat(apath); err != nil || info.Mode() & os.ModeSymlink != 0 {
                t.Error("expected akari to be a copy")
            }

            if _, err := os.Stat(filepath.Join(registry, version_dir, linksFileName)); !errors.Is(err, os.ErrNotExist) {
                t.Error("expected top-level ..links file to no longer exist")
            }
        }

        // Check that unlinked files are unchanged.
        {
            entry, found := man["orange_planet/athena"]
            if !found || entry.Link != nil {
                t.Errorf("unexpected rerouting in manifest; %v", entry)
            }
            apath := filepath.Join(registry, version_dir, "orange_planet", "athena")
            if contents, err := os.ReadFile(apath); err != nil || string(contents) != "glory" {
                t.Errorf("unexpected contents for orange_planet/athena; %v", string(contents))
            }
            if info, err := os.Lstat(apath); err != nil || info.Mode() & os.ModeSymlink != 0 {
                t.Error("expected orange_planet/athena to be its own file")
            }
        }
    })

    t.Run("delete ancestor, reroute grandchild", func(t *testing.T) {
        registry, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        project := "ARIA" 
        asset := "anime"
        err = mockRegistryForReroute(registry, project, asset)
        if err != nil {
            t.Fatal(err)
        }

        to_delete_files, err := listToBeDeletedFiles(registry, map[string]bool{ "ARIA/anime/animation": true })
        if err != nil {
            t.Fatal(err)
        }

        version_dir := filepath.Join(project, asset, "origination")
        proposed, err := proposeLinkReroutes(registry, to_delete_files, version_dir)
        if err != nil {
            t.Fatal(err)
        }

        inverted_changes := invertChangelog(proposed.Actions)
        if len(inverted_changes) != 2 ||
            inverted_changes["ARIA/anime/origination/akari"].Copy || 
            inverted_changes["ARIA/anime/origination/alicia"].Copy {
            t.Errorf("unexpected changelog; %v", proposed.Actions)
        }

        err = executeLinkReroutes(registry, version_dir, proposed)
        if err != nil {
            t.Fatal(err)
        }

        full_vpath := filepath.Join(registry, version_dir)
        man, err := readManifest(full_vpath)
        if err != nil {
            t.Fatal(err)
        }

        // Check that links are rerouted.
        {
            entry, found := man["akari"]
            if !found || entry.Link.Version != "natural" || entry.Link.Ancestor != nil {
                t.Errorf("unexpected rerouting in manifest; %v", entry)
            }

            apath := filepath.Join(registry, version_dir, "akari")
            if contents, err := os.ReadFile(apath); err != nil || string(contents) != "mizunashi" {
                t.Errorf("unexpected contents for akari; %v", string(contents))
            }
            target, err := os.Readlink(apath)
            if err != nil || target != "../natural/akari" {
                t.Errorf("unexpected target for akari; %q", target)
            }

            link_details, err := readLinks(filepath.Join(registry, version_dir))
            if err != nil {
                t.Fatal(err)
            }
            lentry, lfound := link_details["akari"]
            if !lfound || lentry.Version != "natural" || lentry.Ancestor != nil {
                t.Errorf("unexpected rerouting in links file; %v", entry)
            }
        }

        // Check that unlinked files are unchanged.
        {
            entry, found := man["himeya/akira"]
            if !found || entry.Link != nil {
                t.Errorf("unexpected rerouting in manifest; %v", entry)
            }
            apath := filepath.Join(registry, version_dir, "himeya", "akira")
            if contents, err := os.ReadFile(apath); err != nil || string(contents) != "ferrari" {
                t.Errorf("unexpected contents for himeya/akira; %v", string(contents))
            }
            if info, err := os.Lstat(apath); err != nil || info.Mode() & os.ModeSymlink != 0 {
                t.Error("expected himeya/akira to be its own file")
            }
        }
    })

    t.Run("delete ancestor, reroute great-grandchild", func(t *testing.T) {
        registry, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        project := "ARIA" 
        asset := "anime"
        err = mockRegistryForReroute(registry, project, asset)
        if err != nil {
            t.Fatal(err)
        }

        to_delete_files, err := listToBeDeletedFiles(registry, map[string]bool{ "ARIA/anime/animation": true })
        if err != nil {
            t.Fatal(err)
        }

        version_dir := filepath.Join(project, asset, "avvenire")
        proposed, err := proposeLinkReroutes(registry, to_delete_files, version_dir)
        if err != nil {
            t.Fatal(err)
        }

        inverted_changes := invertChangelog(proposed.Actions)
        if len(inverted_changes) != 2 ||
            inverted_changes["ARIA/anime/avvenire/akari"].Copy || 
            inverted_changes["ARIA/anime/avvenire/alicia"].Copy {
            t.Errorf("unexpected changelog; %v", proposed.Actions)
        }

        err = executeLinkReroutes(registry, version_dir, proposed)
        if err != nil {
            t.Fatal(err)
        }

        full_vpath := filepath.Join(registry, version_dir)
        man, err := readManifest(full_vpath)
        if err != nil {
            t.Fatal(err)
        }

        // Check that links are rerouted.
        {
            entry, found := man["alicia"]
            if !found || entry.Link.Version != "origination" || entry.Link.Ancestor == nil || entry.Link.Ancestor.Version != "natural" {
                t.Errorf("unexpected rerouting; %v", entry)
            }

            apath := filepath.Join(registry, version_dir, "alicia")
            if contents, err := os.ReadFile(apath); err != nil || string(contents) != "florence" {
                t.Errorf("unexpected contents for alicia; %v", string(contents))
            }
            target, err := os.Readlink(apath)
            if err != nil || target != "../natural/alicia" {
                t.Errorf("unexpected target for alicia; %q", target)
            }
        }

        // Check that unlinked files are unchanged.
        {
            entry, found := man["ai"]
            if !found || entry.Link != nil {
                t.Errorf("unexpected rerouting in manifest; %v", entry)
            }
            apath := filepath.Join(registry, version_dir, "ai")
            if contents, err := os.ReadFile(apath); err != nil || string(contents) != "aino" {
                t.Errorf("unexpected contents for ai; %v", string(contents))
            }
            if info, err := os.Lstat(apath); err != nil || info.Mode() & os.ModeSymlink != 0 {
                t.Error("expected ai to be its own file")
            }
        }
    })

    t.Run("delete parent and grandparent, reroute great-grandchild", func(t *testing.T) {
        registry, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        project := "ARIA" 
        asset := "anime"
        err = mockRegistryForReroute(registry, project, asset)
        if err != nil {
            t.Fatal(err)
        }

        to_delete_files, err := listToBeDeletedFiles(registry, map[string]bool{ "ARIA/anime/natural": true, "ARIA/anime/origination": true })
        if err != nil {
            t.Fatal(err)
        }

        version_dir := filepath.Join(project, asset, "avvenire")
        proposed, err := proposeLinkReroutes(registry, to_delete_files, version_dir)
        if err != nil {
            t.Fatal(err)
        }

        inverted_changes := invertChangelog(proposed.Actions)
        if len(inverted_changes) != 6 ||
            !inverted_changes["ARIA/anime/avvenire/himeya/akira"].Copy || 
            !inverted_changes["ARIA/anime/avvenire/himeya/aika"].Copy ||
            !inverted_changes["ARIA/anime/avvenire/orange_planet/alice"].Copy || 
            !inverted_changes["ARIA/anime/avvenire/orange_planet/athena"].Copy ||
            inverted_changes["ARIA/anime/avvenire/akari"].Copy || 
            inverted_changes["ARIA/anime/avvenire/alicia"].Copy {
            t.Errorf("unexpected changelog; %v", proposed.Actions)
        }

        err = executeLinkReroutes(registry, version_dir, proposed)
        if err != nil {
            t.Fatal(err)
        }

        full_vpath := filepath.Join(registry, version_dir)
        man, err := readManifest(full_vpath)
        if err != nil {
            t.Fatal(err)
        }

        // Some links are replaced with copies.
        {
            entry, found := man["orange_planet/alice"]
            if !found || entry.Link != nil {
                t.Errorf("unexpected rerouting in manifest; %v", entry)
            }
            apath := filepath.Join(registry, version_dir, "orange_planet", "alice")
            if contents, err := os.ReadFile(apath); err != nil || string(contents) != "carroll" {
                t.Errorf("unexpected contents for akari; %v", string(contents))
            }
            if info, err := os.Lstat(apath); err != nil || info.Mode() & os.ModeSymlink != 0 {
                t.Error("expected orange_planet/alice to be a copy")
            }
        }

        // Other links are rerouted.
        {
            entry, found := man["alicia"]
            if !found || entry.Link == nil || entry.Link.Version != "animation" || entry.Link.Ancestor != nil {
                t.Errorf("unexpected rerouting in manifest; %v", entry)
            }

            apath := filepath.Join(registry, version_dir, "alicia")
            if contents, err := os.ReadFile(apath); err != nil || string(contents) != "florence" {
                t.Errorf("unexpected contents for akari; %v", string(contents))
            }
            target, err := os.Readlink(apath)
            if err != nil || target != "../animation/alicia" {
                t.Errorf("unexpected target for alicia; %q", target)
            }

            link_details, err := readLinks(filepath.Join(registry, version_dir))
            if err != nil {
                t.Fatal(err)
            }
            lentry, lfound := link_details["alicia"]
            if !lfound || lentry.Version != "animation" || lentry.Ancestor != nil {
                t.Errorf("unexpected rerouting in links file; %v", entry)
            }
        }
    })

    t.Run("delete parent and ancestor, reroute great-grandchild", func(t *testing.T) {
        registry, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        project := "ARIA" 
        asset := "anime"
        err = mockRegistryForReroute(registry, project, asset)
        if err != nil {
            t.Fatal(err)
        }

        to_delete_files, err := listToBeDeletedFiles(registry, map[string]bool{ "ARIA/anime/animation": true, "ARIA/anime/origination": true })
        if err != nil {
            t.Fatal(err)
        }

        version_dir := filepath.Join(project, asset, "avvenire")
        proposed, err := proposeLinkReroutes(registry, to_delete_files, version_dir)
        if err != nil {
            t.Fatal(err)
        }

        inverted_changes := invertChangelog(proposed.Actions)
        if len(inverted_changes) != 6 ||
            !inverted_changes["ARIA/anime/avvenire/himeya/akira"].Copy || 
            !inverted_changes["ARIA/anime/avvenire/himeya/aika"].Copy ||
            inverted_changes["ARIA/anime/avvenire/orange_planet/alice"].Copy || 
            inverted_changes["ARIA/anime/avvenire/orange_planet/athena"].Copy ||
            inverted_changes["ARIA/anime/avvenire/akari"].Copy || 
            inverted_changes["ARIA/anime/avvenire/alicia"].Copy {
            t.Errorf("unexpected changelog; %v", proposed.Actions)
        }

        err = executeLinkReroutes(registry, version_dir, proposed)
        if err != nil {
            t.Fatal(err)
        }

        full_vpath := filepath.Join(registry, version_dir)
        man, err := readManifest(full_vpath)
        if err != nil {
            t.Fatal(err)
        }

        // Some links are replaced with copies.
        {
            entry, found := man["himeya/aika"]
            if !found || entry.Link != nil {
                t.Errorf("unexpected rerouting in manifest; %v", entry)
            }
            apath := filepath.Join(registry, version_dir, "himeya", "aika")
            if contents, err := os.ReadFile(apath); err != nil || string(contents) != "granzchesta" {
                t.Errorf("unexpected contents for akari; %v", string(contents))
            }
            if info, err := os.Lstat(apath); err != nil || info.Mode() & os.ModeSymlink != 0 {
                t.Error("expected himeya/aika to be a copy")
            }
        }

        // Other links are rerouted.
        {
            entry, found := man["orange_planet/athena"]
            if !found || entry.Link == nil || entry.Link.Version != "natural" || entry.Link.Ancestor != nil {
                t.Errorf("unexpected rerouting in manifest; %v", entry)
            }

            apath := filepath.Join(registry, version_dir, "orange_planet", "athena")
            if contents, err := os.ReadFile(apath); err != nil || string(contents) != "glory" {
                t.Errorf("unexpected contents for akari; %v", string(contents))
            }
            target, err := os.Readlink(apath)
            if err != nil || target != "../../natural/orange_planet/athena" {
                t.Errorf("unexpected target for orange_planet/athena; %q", target)
            }

            link_details, err := readLinks(filepath.Join(registry, version_dir, "orange_planet"))
            if err != nil {
                t.Fatal(err)
            }
            lentry, lfound := link_details["athena"]
            if !lfound || lentry.Version != "natural" || lentry.Ancestor != nil {
                t.Errorf("unexpected rerouting in links file; %v", entry)
            }
        }
    })

    t.Run("delete grandparent and ancestor, reroute great-grandchild", func(t *testing.T) {
        registry, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        project := "ARIA" 
        asset := "anime"
        err = mockRegistryForReroute(registry, project, asset)
        if err != nil {
            t.Fatal(err)
        }

        to_delete_files, err := listToBeDeletedFiles(registry, map[string]bool{ "ARIA/anime/animation": true, "ARIA/anime/natural": true })
        if err != nil {
            t.Fatal(err)
        }

        version_dir := filepath.Join(project, asset, "avvenire")
        proposed, err := proposeLinkReroutes(registry, to_delete_files, version_dir)
        if err != nil {
            t.Fatal(err)
        }

        inverted_changes := invertChangelog(proposed.Actions)
        if len(inverted_changes) != 4 ||
            inverted_changes["ARIA/anime/avvenire/orange_planet/alice"].Copy || 
            inverted_changes["ARIA/anime/avvenire/orange_planet/athena"].Copy ||
            inverted_changes["ARIA/anime/avvenire/akari"].Copy || 
            inverted_changes["ARIA/anime/avvenire/alicia"].Copy {
            t.Errorf("unexpected changelog; %v", proposed.Actions)
        }

        err = executeLinkReroutes(registry, version_dir, proposed)
        if err != nil {
            t.Fatal(err)
        }

        full_vpath := filepath.Join(registry, version_dir)
        man, err := readManifest(full_vpath)
        if err != nil {
            t.Fatal(err)
        }

        // Check that unlinked files are unchanged.
        {
            entry, found := man["ai"]
            if !found || entry.Link != nil {
                t.Errorf("unexpected rerouting in manifest; %v", entry)
            }
            apath := filepath.Join(registry, version_dir, "ai")
            if contents, err := os.ReadFile(apath); err != nil || string(contents) != "aino" {
                t.Errorf("unexpected contents for ai; %v", string(contents))
            }
            if info, err := os.Lstat(apath); err != nil || info.Mode() & os.ModeSymlink != 0 {
                t.Error("expected ai to be its own file")
            }
        }

        // Links are properly rerouted.
        for fname, expected_contents := range map[string]string{ "alicia":"florence", "orange_planet/athena":"glory", "himeya/akira":"ferrari" } {
            entry, found := man[fname]
            if !found || entry.Link == nil || entry.Link.Version != "origination" || entry.Link.Ancestor != nil {
                t.Errorf("unexpected rerouting in manifest; %v", entry)
            }

            fpath := filepath.Join(registry, version_dir, fname)
            if contents, err := os.ReadFile(fpath); err != nil || string(contents) != expected_contents {
                t.Errorf("unexpected contents for %q; %v", fname, string(contents))
            }
            target, err := os.Readlink(fpath)
            if err != nil {
                t.Error(err)
            }

            dirname := filepath.Dir(fname)
            expected_link := "../origination/" + fname
            if dirname != "." {
                expected_link = "../" + expected_link
            }
            if target != expected_link {
                t.Errorf("unexpected target for %s versus %s; %q", fname, expected_link, target)
            }

            link_details, err := readLinks(filepath.Join(registry, version_dir, filepath.Dir(fname)))
            if err != nil {
                t.Fatal(err)
            }
            lentry, lfound := link_details[filepath.Base(fname)]
            if !lfound || lentry.Version != "origination" || lentry.Ancestor != nil {
                t.Errorf("unexpected rerouting in links file; %v", entry)
            }
        }
    })

    t.Run("delete all but the last, reroute great-grandchild", func(t *testing.T) {
        registry, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        project := "ARIA" 
        asset := "anime"
        err = mockRegistryForReroute(registry, project, asset)
        if err != nil {
            t.Fatal(err)
        }

        to_delete_files, err := listToBeDeletedFiles(registry, map[string]bool{ "ARIA/anime/animation": true, "ARIA/anime/natural": true, "ARIA/anime/origination": true })
        if err != nil {
            t.Fatal(err)
        }

        version_dir := filepath.Join(project, asset, "avvenire")
        proposed, err := proposeLinkReroutes(registry, to_delete_files, version_dir)
        if err != nil {
            t.Fatal(err)
        }

        inverted_changes := invertChangelog(proposed.Actions)
        if len(inverted_changes) != 6 ||
            !inverted_changes["ARIA/anime/avvenire/himeya/akira"].Copy || 
            !inverted_changes["ARIA/anime/avvenire/himeya/aika"].Copy ||
            !inverted_changes["ARIA/anime/avvenire/orange_planet/alice"].Copy || 
            !inverted_changes["ARIA/anime/avvenire/orange_planet/athena"].Copy ||
            !inverted_changes["ARIA/anime/avvenire/akari"].Copy || 
            !inverted_changes["ARIA/anime/avvenire/alicia"].Copy {
            t.Errorf("unexpected changelog; %v", proposed.Actions)
        }

        err = executeLinkReroutes(registry, version_dir, proposed)
        if err != nil {
            t.Fatal(err)
        }

        full_vpath := filepath.Join(registry, version_dir)
        man, err := readManifest(full_vpath)
        if err != nil {
            t.Fatal(err)
        }

        // Check that copies are made of all files.
        for fname, expected_contents := range map[string]string{ "akari":"mizunashi", "orange_planet/alice":"carroll", "himeya/aika":"granzchesta" } {
            entry, found := man[fname]
            if !found || entry.Link != nil {
                t.Errorf("unexpected rerouting in manifest; %v", entry)
            }
            apath := filepath.Join(registry, version_dir, fname)
            if contents, err := os.ReadFile(apath); err != nil || string(contents) != expected_contents {
                t.Errorf("unexpected contents for %s; %v", fname, string(contents))
            }
            if info, err := os.Lstat(apath); err != nil || info.Mode() & os.ModeSymlink != 0 {
                t.Errorf("expected %s to be its own file", fname)
            }
        }
    })

    t.Run("dry run", func(t *testing.T) {
        registry, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        project := "ARIA" 
        asset := "anime"
        err = mockRegistryForReroute(registry, project, asset)
        if err != nil {
            t.Fatal(err)
        }

        to_delete_files, err := listToBeDeletedFiles(registry, map[string]bool{ "ARIA/anime/animation": true, "ARIA/anime/origination": true })
        if err != nil {
            t.Fatal(err)
        }

        version_dir := filepath.Join(project, asset, "avvenire")
        proposed, err := proposeLinkReroutes(registry, to_delete_files, version_dir)
        if err != nil {
            t.Fatal(err)
        }

        inverted_changes := invertChangelog(proposed.Actions)
        if len(inverted_changes) != 6 ||
            !inverted_changes["ARIA/anime/avvenire/himeya/akira"].Copy || 
            !inverted_changes["ARIA/anime/avvenire/himeya/aika"].Copy ||
            inverted_changes["ARIA/anime/avvenire/orange_planet/alice"].Copy || 
            inverted_changes["ARIA/anime/avvenire/orange_planet/athena"].Copy ||
            inverted_changes["ARIA/anime/avvenire/akari"].Copy || 
            inverted_changes["ARIA/anime/avvenire/alicia"].Copy {
            t.Errorf("unexpected changelog; %v", proposed.Actions)
        }

        // No executeLinkReroutes() call here, so everything in the registry should still be unchanged.

        full_vpath := filepath.Join(registry, version_dir)
        man, err := readManifest(full_vpath)
        if err != nil {
            t.Fatal(err)
        }

        // Check that to-be-copied files are still symlinks.
        {
            entry, found := man["himeya/aika"]
            if !found || entry.Link == nil {
                t.Errorf("unexpected rerouting in dry-run manifest; %v", entry)
            }
            apath := filepath.Join(registry, version_dir, "himeya/aika")
            if info, err := os.Lstat(apath); err != nil || info.Mode() & os.ModeSymlink == 0 {
                t.Errorf("expected himeya/akia to be its a symlink after a dry-run")
            }
        }

        // Check that to-be-relinked symlinks still point to the original location.
        {
            entry, found := man["alicia"]
            if !found || entry.Link == nil {
                t.Errorf("unexpected rerouting in dry-run manifest; %v", entry)
            }
            apath := filepath.Join(registry, version_dir, "alicia")
            target, err := os.Readlink(apath)
            if err != nil || target != "../animation/alicia" {
                t.Errorf("expected alicia symlink to still point to 'animation'")
            }
        }
    })
}

func TestRerouteLinksHandler(t *testing.T) {
    mock, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create a mock temp directory; %v", err)
    }
    self, err := identifyUser(mock)
    if err != nil {
        t.Fatalf("failed to identify self; %v", err)
    }

    t.Run("basic", func(t *testing.T) {
        registry, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        project := "ARIA" 
        asset := "anime"
        err = mockRegistryForReroute(registry, project, asset)
        if err != nil {
            t.Fatal(err)
        }

        reqpath, err := dumpRequest("reroute_links", fmt.Sprintf(`{
    "to_delete": [ 
        { "project": "%s", "asset": "%s", "version": "origination" } 
    ]
}`, project, asset))
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        globals := newGlobalConfiguration(registry)
        globals.Administrators = append(globals.Administrators, self)
        changes, err := rerouteLinksHandler(reqpath, &globals)
        if err != nil {
            t.Fatal(err)
        }

        inverted_changes := invertChangelog(changes)
        if len(inverted_changes) != 6 ||
            !inverted_changes["ARIA/anime/avvenire/himeya/akira"].Copy || 
            !inverted_changes["ARIA/anime/avvenire/himeya/aika"].Copy ||
            inverted_changes["ARIA/anime/avvenire/orange_planet/alice"].Copy || 
            inverted_changes["ARIA/anime/avvenire/orange_planet/athena"].Copy ||
            inverted_changes["ARIA/anime/avvenire/akari"].Copy || 
            inverted_changes["ARIA/anime/avvenire/alicia"].Copy {
            t.Errorf("unexpected changelog; %v", changes)
        }

        version_dir := filepath.Join(project, asset, "avvenire")
        full_vpath := filepath.Join(registry, version_dir)
        man, err := readManifest(full_vpath)
        if err != nil {
            t.Fatal(err)
        }

        // Some links are replaced with copies.
        {
            entry, found := man["himeya/aika"]
            if !found || entry.Link != nil {
                t.Errorf("unexpected rerouting in manifest; %v", entry)
            }
            apath := filepath.Join(registry, version_dir, "himeya", "aika")
            if contents, err := os.ReadFile(apath); err != nil || string(contents) != "granzchesta" {
                t.Errorf("unexpected contents for himeya/aika; %v", string(contents))
            }
            if info, err := os.Lstat(apath); err != nil || info.Mode() & os.ModeSymlink != 0 {
                t.Error("expected himeya/aika to be a copy")
            }
        }

        // Other links are rerouted.
        {
            entry, found := man["orange_planet/alice"]
            if !found || entry.Link == nil || entry.Link.Version != "natural" || entry.Link.Ancestor != nil {
                t.Errorf("unexpected rerouting in manifest; %v", entry)
            }
            apath := filepath.Join(registry, version_dir, "orange_planet", "alice")
            if contents, err := os.ReadFile(apath); err != nil || string(contents) != "carroll" {
                t.Errorf("unexpected contents for orange_planet/alice; %v", string(contents))
            }
            target, err := os.Readlink(apath)
            if err != nil || target != "../../natural/orange_planet/alice" {
                t.Errorf("unexpected target for orange_planet/alice; %q", target)
            }
        }
    })

    t.Run("multiple", func(t *testing.T) {
        registry, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        project := "ARIA" 
        asset := "anime"
        err = mockRegistryForReroute(registry, project, asset)
        if err != nil {
            t.Fatal(err)
        }

        old_usage, err := readUsage(filepath.Join(registry, project))
        if err != nil{
            t.Fatal(err)
        }

        reqpath, err := dumpRequest("reroute_links", fmt.Sprintf(`{
    "to_delete": [ 
        { "project": "%s", "asset": "%s", "version": "animation" },
        { "project": "%s", "asset": "%s", "version": "natural" }
    ]
}`, project, asset, project, asset))
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        globals := newGlobalConfiguration(registry)
        globals.Administrators = append(globals.Administrators, self)
        changes, err := rerouteLinksHandler(reqpath, &globals)
        if err != nil {
            t.Fatal(err)
        }

        inverted_changes := invertChangelog(changes)
        if len(inverted_changes) != 8 ||
            !inverted_changes["ARIA/anime/origination/orange_planet/alice"].Copy || 
            !inverted_changes["ARIA/anime/origination/orange_planet/athena"].Copy ||
            !inverted_changes["ARIA/anime/origination/akari"].Copy || 
            !inverted_changes["ARIA/anime/origination/alicia"].Copy ||
            inverted_changes["ARIA/anime/avvenire/orange_planet/alice"].Copy || 
            inverted_changes["ARIA/anime/avvenire/orange_planet/athena"].Copy ||
            inverted_changes["ARIA/anime/avvenire/akari"].Copy || 
            inverted_changes["ARIA/anime/avvenire/alicia"].Copy {
            t.Errorf("unexpected changelog; %v", changes)
        }

        // Some links are replaced with copies.
        {
            version_dir := filepath.Join(project, asset, "origination")
            full_vpath := filepath.Join(registry, version_dir)
            man, err := readManifest(full_vpath)
            if err != nil {
                t.Fatal(err)
            }

            entry, found := man["orange_planet/athena"]
            if !found || entry.Link != nil {
                t.Errorf("unexpected rerouting in manifest; %v", entry)
            }
            apath := filepath.Join(registry, version_dir, "orange_planet", "athena")
            if contents, err := os.ReadFile(apath); err != nil || string(contents) != "glory" {
                t.Errorf("unexpected contents for orange_planet/athena; %v", string(contents))
            }
            if info, err := os.Lstat(apath); err != nil || info.Mode() & os.ModeSymlink != 0 {
                t.Error("expected orange_planet/athena to be a copy")
            }
        }

        // Other links are rerouted.
        {
            version_dir := filepath.Join(project, asset, "avvenire")
            full_vpath := filepath.Join(registry, version_dir)
            man, err := readManifest(full_vpath)
            if err != nil {
                t.Fatal(err)
            }
            entry, found := man["orange_planet/alice"]
            if !found || entry.Link == nil || entry.Link.Version != "origination" || entry.Link.Ancestor != nil {
                t.Errorf("unexpected rerouting in manifest; %v", entry)
            }
            apath := filepath.Join(registry, version_dir, "orange_planet", "alice")
            if contents, err := os.ReadFile(apath); err != nil || string(contents) != "carroll" {
                t.Errorf("unexpected contents for orange_planet/alice; %v", string(contents))
            }
            target, err := os.Readlink(apath)
            if err != nil || target != "../../origination/orange_planet/alice" {
                t.Errorf("unexpected target for orange_planet/alice; %q", target)
            }
        }

        // Check that usage increases according to all the files that were copied in 'changes'.
        new_usage, err := readUsage(filepath.Join(registry, project))
        if err != nil{
            t.Fatal(err)
        }
        diff_usage := new_usage.Total - old_usage.Total
        if diff_usage != int64(len("carroll") + len("glory") + len("mizunashi") + len("florence")) {
            t.Errorf("unexpected increase in usage; %v versus %v", new_usage.Total, old_usage.Total)
        }
    })

    t.Run("dry run", func(t *testing.T) {
        registry, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        project := "ARIA" 
        asset := "anime"
        err = mockRegistryForReroute(registry, project, asset)
        if err != nil {
            t.Fatal(err)
        }

        old_usage, err := readUsage(filepath.Join(registry, project))
        if err != nil{
            t.Fatal(err)
        }

        reqpath, err := dumpRequest("reroute_links", fmt.Sprintf(`{
    "to_delete": [ 
        { "project": "%s", "asset": "%s", "version": "natural" } 
    ],
    "dry_run": true
}`, project, asset))
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        globals := newGlobalConfiguration(registry)
        globals.Administrators = append(globals.Administrators, self)
        changes, err := rerouteLinksHandler(reqpath, &globals)
        if err != nil {
            t.Fatal(err)
        }

        inverted_changes := invertChangelog(changes)
        if len(inverted_changes) != 6 ||
            !inverted_changes["ARIA/anime/origination/orange_planet/alice"].Copy || 
            !inverted_changes["ARIA/anime/origination/orange_planet/athena"].Copy ||
            inverted_changes["ARIA/anime/origination/akari"].Copy || 
            inverted_changes["ARIA/anime/origination/alicia"].Copy ||
            inverted_changes["ARIA/anime/avvenire/orange_planet/alice"].Copy || 
            inverted_changes["ARIA/anime/avvenire/orange_planet/athena"].Copy {
            t.Errorf("unexpected changelog; %v", changes)
        }

        // Nothing is actually changed in a dry run.
        {
            version_dir := filepath.Join(project, asset, "origination")
            full_vpath := filepath.Join(registry, version_dir)
            man, err := readManifest(full_vpath)
            if err != nil {
                t.Fatal(err)
            }

            entry, found := man["orange_planet/athena"]
            if !found || entry.Link == nil {
                t.Errorf("unexpected rerouting in manifest; %v", entry)
            }

            apath := filepath.Join(registry, version_dir, "orange_planet", "athena")
            if info, err := os.Lstat(apath); err != nil || info.Mode() & os.ModeSymlink == 0 {
                t.Error("expected himeya/aika to still be a symlink")
            }
        }

        // Again, nothing is changed in the great grandchild.
        {
            version_dir := filepath.Join(project, asset, "avvenire")
            full_vpath := filepath.Join(registry, version_dir)
            man, err := readManifest(full_vpath)
            if err != nil {
                t.Fatal(err)
            }

            entry, found := man["orange_planet/athena"]
            if !found || entry.Link == nil {
                t.Errorf("unexpected rerouting in manifest; %v", entry)
            }

            apath := filepath.Join(registry, version_dir, "orange_planet", "athena")
            target, err := os.Readlink(apath)
            if err != nil || target == "../natural/orange_planet/athena" {
                t.Errorf("expected orange_planet/athena symlink to still point to 'natural'")
            }
        }

        new_usage, err := readUsage(filepath.Join(registry, project))
        if err != nil{
            t.Fatal(err)
        }
        if new_usage.Total != old_usage.Total {
            t.Error("usage should not change after a dry run")
        }
    })

    t.Run("unauthorized", func(t *testing.T) {
        registry, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        project := "ARIA" 
        asset := "anime"
        err = mockRegistryForReroute(registry, project, asset)
        if err != nil {
            t.Fatal(err)
        }

        reqpath, err := dumpRequest("reroute_links", `{ "to_delete": [ { "project": "ARIA" } ] }`)
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        globals := newGlobalConfiguration(registry)
        _, err = rerouteLinksHandler(reqpath, &globals)
        if err == nil || !strings.Contains(err.Error(), "not authorized") {
            t.Error("unexpected authorization for non-admin")
        }
    })

    t.Run("bad request", func(t *testing.T) {
        registry, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        project := "ARIA" 
        asset := "anime"
        err = mockRegistryForReroute(registry, project, asset)
        if err != nil {
            t.Fatal(err)
        }

        globals := newGlobalConfiguration(registry)
        globals.Administrators = append(globals.Administrators, self)

        reqpath, err := dumpRequest("reroute_links", "{}")
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }
        _, err = rerouteLinksHandler(reqpath, &globals)
        if err == nil || !strings.Contains(err.Error(), "'to_delete'") {
            t.Error("expected failure when to_delete isn't present")
        }

        reqpath, err = dumpRequest("reroute_links", `{ "to_delete": [ { "project": "" } ] }`)
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }
        _, err = rerouteLinksHandler(reqpath, &globals)
        if err == nil || !strings.Contains(err.Error(), "invalid 'project'") {
            t.Error("expected failure from invalid project")
        }

        reqpath, err = dumpRequest("reroute_links", `{ "to_delete": [ { "project": "ARIA", "asset": "" } ] }`)
        _, err = rerouteLinksHandler(reqpath, &globals)
        if err == nil || !strings.Contains(err.Error(), "invalid 'asset'") {
            t.Error("expected failure from invalid asset")
        }

        reqpath, err = dumpRequest("reroute_links", `{ "to_delete": [ { "project": "ARIA", "asset": "anime", "version": "" } ] }`)
        _, err = rerouteLinksHandler(reqpath, &globals)
        if err == nil || !strings.Contains(err.Error(), "invalid 'version'") {
            t.Error("expected failure from invalid version")
        }

        reqpath, err = dumpRequest("reroute_links", `{ "to_delete": [ { "project": "ARIA", "version": "origination" } ] }`)
        _, err = rerouteLinksHandler(reqpath, &globals)
        if err == nil || !strings.Contains(err.Error(), "requires the 'asset'") {
            t.Error("expected failure from version without asset")
        }
    })
}
