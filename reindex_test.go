package main

import (
    "testing"
    "os"
    "path/filepath"
    "fmt"
    "os/user"
    "strings"
)

func setupDirectoryForReindexTest(globals *globalConfiguration, project, asset, version string) error {
    self, err := user.Current()
    if err != nil {
        return fmt.Errorf("failed to determine the current user; %w", err)
    }

    err = createProject(project, nil, self.Username, globals)
    if err != nil {
        return err
    }

    dir := filepath.Join(globals.Registry, project, asset, version)
    err = os.MkdirAll(dir, 0755)
    if err != nil {
        return err
    }

    err = os.WriteFile(filepath.Join(dir, summaryFileName), []byte(`{ 
    "upload_user_id": "aaron",
    "upload_start": "2025-01-26T11:28:10Z",
    "upload_finish": "2025-01-26T11:28:20Z"
}`), 0644)

    err = os.WriteFile(filepath.Join(dir, "evolution"), []byte("haunter"), 0644)
    if err != nil {
        return err
    }

    err = os.WriteFile(filepath.Join(dir, "moves"), []byte("lick,confuse_ray,shadow_ball,dream_eater"), 0644)
    if err != nil {
        return err
    }

    return nil
}

func TestReindexHandlerSimple(t *testing.T) {
    project := "original_series"
    asset := "gastly"
    version := "lavender"

    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg)

    err = setupDirectoryForReindexTest(&globals, project, asset, version)
    if err != nil {
        t.Fatalf("failed to set up project directory; %v", err)
    }

    // Performing the request.
    req_string := fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version)
    reqname, err := dumpRequest("reindex", req_string)
    if err != nil {
        t.Fatalf("failed to create reindex request; %v", err)
    }

    err = reindexHandler(reqname, &globals)
    if err != nil {
        t.Fatalf("failed to perform the reindexing; %v", err)
    }

    // Checking a few manifest entries and files.
    destination := filepath.Join(reg, project, asset, version)
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
        logs[0].Latest == nil || !*(logs[0].Latest) {
        t.Fatalf("unexpected contents for first log in %q", reg)
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
    globals := newGlobalConfiguration(reg)

    err = setupDirectoryForReindexTest(&globals, project, asset, version)
    if err != nil {
        t.Fatalf("failed to set up project directory; %v", err)
    }

    // Set it to be on probation.
    err = os.WriteFile(filepath.Join(globals.Registry, project, asset, version, summaryFileName), []byte(`{ 
    "upload_user_id": "aaron",
    "upload_start": "2025-01-26T11:28:10Z",
    "upload_finish": "2025-01-26T11:28:20Z",
    "on_probation": true
}`), 0644)

    // Performing the request.
    req_string := fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version)
    reqname, err := dumpRequest("reindex", req_string)
    if err != nil {
        t.Fatalf("failed to create reindex request; %v", err)
    }

    err = reindexHandler(reqname, &globals)
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
    globals := newGlobalConfiguration(reg)

    err = setupDirectoryForReindexTest(&globals, project, asset, version)
    if err != nil {
        t.Fatalf("failed to set up project directory; %v", err)
    }

    t.Run("bad project", func(t *testing.T) {
        reqname, err := dumpRequest("reindex", `{ "asset": "foo", "version": "bar" }`)
        if err != nil {
            t.Fatalf("failed to create reindex request; %v", err)
        }
        err = reindexHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "expected a 'project'") {
            t.Fatal("configuration should fail for missing project")
        }

        reqname, err = dumpRequest("reindex", `{ "project": "bad/name", "asset": "foo", "version": "bar" }`)
        if err != nil {
            t.Fatalf("failed to create reindex request; %v", err)
        }
        err = reindexHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "invalid project name") {
            t.Fatal("configuration should fail for invalid project name")
        }
    })

    t.Run("bad asset", func(t *testing.T) {
        reqname, err := dumpRequest("reindex", `{ "project": "foo", "version": "bar" }`)
        if err != nil {
            t.Fatalf("failed to create reindex request; %v", err)
        }
        err = reindexHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "expected an 'asset'") {
            t.Fatal("configuration should fail for missing asset")
        }

        reqname, err = dumpRequest("reindex", `{ "project": "foo", "asset": "..bar", "version": "bar" }`)
        if err != nil {
            t.Fatalf("failed to create reindex request; %v", err)
        }
        err = reindexHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "invalid asset name") {
            t.Fatal("configuration should fail for invalid asset name")
        }
    })

    t.Run("bad version", func(t *testing.T) {
        reqname, err := dumpRequest("reindex", `{ "project": "foo", "asset": "bar" }`) 
        if err != nil {
            t.Fatalf("failed to create reindex request; %v", err)
        }
        err = reindexHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "expected a 'version'") {
            t.Fatal("configuration should fail for missing version")
        }

        reqname, err = dumpRequest("reindex", `{ "project": "foo", "asset": "bar", "version": "" }`)
        if err != nil {
            t.Fatalf("failed to create reindex request; %v", err)
        }
        err = reindexHandler(reqname, &globals)
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
    globals := newGlobalConfiguration(reg)

    err = setupDirectoryForReindexTest(&globals, project, asset, version)
    if err != nil {
        t.Fatalf("failed to set up project directory; %v", err)
    }

    // Wiping the user.
    err = os.WriteFile(filepath.Join(globals.Registry, project, permissionsFileName), []byte(`{ "owners": [], "uploaders": [] }`), 0644)
    if err != nil {
        t.Fatalf("failed to edit the owners; %v", err)
    }

    req_string := fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version)
    reqname, err := dumpRequest("reindex", req_string)
    if err != nil {
        t.Fatalf("failed to create reindex request; %v", err)
    }

    err = reindexHandler(reqname, &globals)
    if err == nil || !strings.Contains(err.Error(), "not authorized") {
        t.Fatalf("failed to reject reindex from non-authorized user")
    }
}
