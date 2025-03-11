package main

import (
    "testing"
    "os"
    "path/filepath"
    "fmt"
    "os/user"
    "time"
    "errors"
    "encoding/json"
    "strings"
)

func setupSourceForUploadTest() (string, error) {
    src, err := os.MkdirTemp("", "")
    if err != nil {
        return "", fmt.Errorf("failed to create the temporary directory; %w", err)
    }

    err = os.WriteFile(filepath.Join(src, "evolution"), []byte("haunter"), 0644)
    if err != nil {
        return "", err
    }

    err = os.WriteFile(filepath.Join(src, "moves"), []byte("lick,confuse_ray,shadow_ball,dream_eater"), 0644)
    if err != nil {
        return "", err
    }

    return src, nil
}

func setupProjectForUploadTest(project string, globals *globalConfiguration) error {
    self, err := user.Current()
    if err != nil {
        return fmt.Errorf("failed to determine the current user; %w", err)
    }

    err = createProject(project, nil, self.Username, globals)
    if err != nil {
        return err
    }

    return nil
}

func TestUploadHandlerSimple(t *testing.T) {
    project := "original_series"
    asset := "gastly"
    version := "lavender"

    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg)

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    err = setupProjectForUploadTest(project, &globals)
    if err != nil {
        t.Fatalf("failed to set up project directory; %v", err)
    }

    req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, filepath.Base(src), project, asset, version)
    reqname, err := dumpRequest("upload", req_string)
    if err != nil {
        t.Fatalf("failed to create upload request; %v", err)
    }

    err = uploadHandler(reqname, &globals)
    if err != nil {
        t.Fatalf("failed to perform the upload; %v", err)
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

    // Checking out the summary.
    summ, err := readSummary(destination)
    if err != nil {
        t.Fatalf("failed to read the summary; %v", err)
    }

    self, err := user.Current()
    if err != nil {
        t.Fatalf("failed to determine the current user; %v", err)
    }
    if summ.UploadUserId != self.Username {
        t.Fatalf("user in summary is not as expected (expected %q, got %q)", self.Username, summ.UploadUserId)
    }

    ustart, err := time.Parse(time.RFC3339, summ.UploadStart)
    if err != nil {
        t.Fatalf("upload start is not a valid time; %v", err)
    }
    ufinish, err := time.Parse(time.RFC3339, summ.UploadFinish)
    if err != nil {
        t.Fatalf("upload finish is not a valid time; %v", err)
    }
    if ustart.After(ufinish) {
        t.Fatalf("upload finish should be at or after the upload start; %v", err)
    }

    if summ.OnProbation != nil {
        t.Fatal("no probation property should be present")
    }

    // Checking out the usage.
    project_dir := filepath.Join(reg, project)
    used, err := readUsage(project_dir)
    if err != nil {
        t.Fatalf("failed to read the usage; %v", err)
    }
    expected_usage, err := computeProjectUsage(project_dir)
    if err != nil {
        t.Fatalf("failed to compute the expected usage; %v", err)
    }
    if expected_usage != used.Total {
        t.Fatalf("unexpected usage total (expected %d, got %d)", expected_usage, used.Total)
    }
    manifest_sum := int64(0)
    for _, m := range man {
        manifest_sum += m.Size
    }
    if expected_usage != manifest_sum {
        t.Fatalf("usage total does not match with sum of sizes in manifest (expected %d, got %d)", expected_usage, manifest_sum)
    }

    // Checking out the latest version.
    latest, err := readLatest(filepath.Join(reg, project, asset))
    if err != nil {
        t.Fatalf("failed to read the latest; %v", err)
    }
    if latest.Version != version {
        t.Fatalf("unexpected latest version (expected %q, got %q)", latest.Version, version)
    }

    quota_raw, err := os.ReadFile(filepath.Join(reg, project, "..quota"))
    if err != nil {
        t.Fatalf("failed to read the quota; %v", err)
    }
    deets := struct {
        Baseline int `json:"baseline"`
        GrowthRate int `json:"growth_rate"`
        Year int `json:"year"`
    }{}
    err = json.Unmarshal(quota_raw, &deets)
    if err != nil {
        t.Fatalf("failed to parse the quota; %v", err)
    }
    if !(deets.Baseline > 0 && deets.GrowthRate > 0 && deets.Year > 0) {
        t.Fatalf("uninitialized fields in the quota")
    }

    // Checking that the logs have something in them.
    logs, err := readAllLogs(reg)
    if err != nil {
        t.Fatalf("failed to read the logs; %v", err)
    }
    if len(logs) != 1 {
        t.Fatalf("expected exactly one entry in the log directory")
    }
    if logs[0].Type != "add-version" || 
        logs[0].Project == nil || *(logs[0].Project) != project || 
        logs[0].Asset == nil || *(logs[0].Asset) != asset || 
        logs[0].Version == nil || *(logs[0].Version) != version || 
        logs[0].Latest == nil || !*(logs[0].Latest) {
        t.Fatalf("unexpected contents for first log in %q", reg)
    }
}

func TestUploadHandlerSimpleFailures(t *testing.T) {
    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg)

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    t.Run("bad source", func(t *testing.T) {
        project := "test"
        asset := "gastly"
        version := "lavender"

        err := setupProjectForUploadTest(project, &globals)
        if err != nil {
            t.Fatalf("failed to set up project directory; %v", err)
        }

        t.Run("no source", func(t *testing.T) {
            req_string := fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version)
            reqname, err := dumpRequest("upload", req_string)
            if err != nil {
                t.Fatalf("failed to create upload request; %v", err)
            }
            err = uploadHandler(reqname, &globals)
            if err == nil || !strings.Contains(err.Error(), "expected a 'source'") {
                t.Fatalf("configuration should have failed without a source")
            }
        })

        t.Run("not name", func(t *testing.T) {
            req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, src, project, asset, version)
            reqname, err := dumpRequest("upload", req_string)
            if err != nil {
                t.Fatalf("failed to create upload request; %v", err)
            }

            err = uploadHandler(reqname, &globals)
            if err == nil || !strings.Contains(err.Error(), "not a path") {
                t.Fatalf("configuration should have failed if the source is a path instead of a name")
            }
        })

        // Source does not exist.
        t.Run("non-existent", func(t *testing.T) {
            alt, err := os.MkdirTemp("", "")
            if err != nil {
                t.Fatal(err)
            }
            err = os.Remove(alt)
            if err != nil {
                t.Fatal(err)
            }

            req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, filepath.Base(alt), project, asset, version)
            reqname, err := dumpRequest("upload", req_string)
            if err != nil {
                t.Fatalf("failed to create upload request; %v", err)
            }
            err = uploadHandler(reqname, &globals)
            if err == nil || !strings.Contains(err.Error(), "failed to stat") {
                t.Fatal("configuration should have failed if the source does not exist")
            }
        })

        // Source is not a directory.
        t.Run("not directory", func(t *testing.T) {
            handle, err := os.CreateTemp("", "")
            if err != nil {
                t.Fatal(err)
            }
            alt := handle.Name()
            handle.Close()

            req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, filepath.Base(alt), project, asset, version)
            reqname, err := dumpRequest("upload", req_string)
            if err != nil {
                t.Fatalf("failed to create upload request; %v", err)
            }
            err = uploadHandler(reqname, &globals)
            if err == nil || !strings.Contains(err.Error(), "be a directory") {
                t.Fatal("configuration should have failed if the source is not a directory")
            }
        })
    })

    t.Run("bad project", func(t *testing.T) {
        req_string := fmt.Sprintf(`{ "source": "%s", "asset": "foo", "version": "bar" }`, filepath.Base(src))
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }
        err = uploadHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "expected a 'project'") {
            t.Fatal("configuration should fail for missing project")
        }

        req_string = fmt.Sprintf(`{ "source": "%s", "project": "bad/name", "asset": "foo", "version": "bar" }`, filepath.Base(src))
        reqname, err = dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }
        err = uploadHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "invalid project name") {
            t.Fatal("configuration should fail for invalid project name")
        }
    })

    t.Run("bad asset", func(t *testing.T) {
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "foo", "version": "bar" }`, filepath.Base(src))
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }
        err = uploadHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "expected an 'asset'") {
            t.Fatal("configuration should fail for missing asset")
        }

        req_string = fmt.Sprintf(`{ "source": "%s", "project": "foo", "asset": "..bar", "version": "bar" }`, filepath.Base(src))
        reqname, err = dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }
        err = uploadHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "invalid asset name") {
            t.Fatal("configuration should fail for invalid asset name")
        }
    })

    t.Run("bad version", func(t *testing.T) {
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "foo", "asset": "bar" }`, filepath.Base(src))
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }
        err = uploadHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "expected a 'version'") {
            t.Fatal("configuration should fail for missing version")
        }

        req_string = fmt.Sprintf(`{ "source": "%s", "project": "foo", "asset": "bar", "version": "" }`, filepath.Base(src))
        reqname, err = dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }
        err = uploadHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "invalid version name") {
            t.Fatal("configuration should fail for invalid version name")
        }
    })
}

func TestUploadHandlerUpdate(t *testing.T) {
    project := "original_series"
    asset := "gastly"
    version := "lavender"

    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg)

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    err = setupProjectForUploadTest(project, &globals)
    if err != nil {
        t.Fatalf("failed to set up project directory; %v", err)
    }

    // Uploading the first version.
    old_usage := int64(0)
    {
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, filepath.Base(src), project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = uploadHandler(reqname, &globals)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        used, err := readUsage(filepath.Join(reg, project))
        if err != nil {
            t.Fatalf("failed to read the usage; %v", err)
        }
        old_usage = used.Total
    }

    // Executing another transfer on a different version.
    t.Run("new version", func(t *testing.T) {
        version := "cerulean"
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, filepath.Base(src), project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        all_evos := "haunter,gengar"
        err = os.WriteFile(filepath.Join(src, "evolution"), []byte(all_evos), 0644)
        if err != nil {
            t.Fatalf("failed to update the 'evolution' file; %v", err)
        }

        err = uploadHandler(reqname, &globals)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        destination := filepath.Join(reg, project, asset, version)
        man, err := readManifest(destination)
        if err != nil {
            t.Fatalf("failed to read the manifest; %v", err)
        }
        info, ok := man["evolution"]
        if !ok || int(info.Size) != len(all_evos) || info.Link != nil {
            t.Fatal("unexpected manifest entry for 'evolution'")
        }
        minfo, ok := man["moves"]
        if !ok || minfo.Link == nil {
            t.Fatal("expected a link for 'moves' in the manifest")
        }
        err = verifyFileContents(filepath.Join(destination, "evolution"), all_evos)
        if err != nil {
            t.Fatalf("could not verify 'evolution'; %v", err)
        }

        // Ensuring that the usage accumulates.
        project_dir := filepath.Join(reg, project)
        usage, err := readUsage(project_dir)
        if err != nil {
            t.Fatalf("failed to read the usage; %v", err)
        }
        expected_usage, err := computeProjectUsage(project_dir)
        if err != nil {
            t.Fatalf("failed to compute the expected usage; %v", err)
        }
        if expected_usage != usage.Total {
            t.Fatalf("unexpected usage total (expected %d, got %d)", expected_usage, usage.Total)
        }
        for _, m := range man {
            if m.Link == nil {
                old_usage += m.Size
            }
        }
        if usage.Total != old_usage {
            t.Fatalf("usage total should equal the sum of non-link sizes (expected %d, got %d)", old_usage, usage.Total)
        }

        // Confirming that we updated to the latest version.
        latest, err := readLatest(filepath.Join(reg, project, asset))
        if err != nil {
            t.Fatalf("failed to read the latest; %v", err)
        }
        if latest.Version != version {
            t.Fatalf("unexpected latest version (expected %q, got %q)", version, latest.Version)
        }
    })

    t.Run("new version", func(t *testing.T) {
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, filepath.Base(src), project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = uploadHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "already exists") {
            t.Fatal("configuration should fail for an existing version")
        }
    })
}

func setupProjectForUploadTestWithPermissions(project string, owners []string, uploaders []unsafeUploaderEntry, globals *globalConfiguration) error {
    self, err := user.Current()
    if err != nil {
        return fmt.Errorf("failed to determine the current user; %w", err)
    }

    err = createProject(project, &unsafePermissionsMetadata{ Owners: owners, Uploaders: uploaders }, self.Username, globals) 
    if err != nil {
        return err
    }

    return nil
}

func TestUploadHandlerUnauthorized(t *testing.T) {
    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg)

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Making a project with explicitly no permissions.
    project := "aaron"
    err = setupProjectForUploadTestWithPermissions(project, []string{}, nil, &globals)
    if err != nil {
        t.Fatalf("failed to create the project; %v", err)
    }

    // Now making the request.
    asset := "BAR"
    version := "whee"
    req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, filepath.Base(src), project, asset, version)
    reqname, err := dumpRequest("upload", req_string)
    if err != nil {
        t.Fatalf("failed to create upload request; %v", err)
    }

    err = uploadHandler(reqname, &globals)
    if err == nil || !strings.Contains(err.Error(), "not authorized") {
        t.Fatalf("failed to reject upload from non-authorized user")
    }
}

func TestUploadHandlerGlobalWrite(t *testing.T) {
    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg)

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Making a project with global write permissions.
    project := "hyperbole"
    self, err := user.Current()
    if err != nil {
        t.Fatalf("failed to determine the current user; %v", err)
    }

    global_write := true
    err = createProject(project, &unsafePermissionsMetadata{ Owners: []string{}, Uploaders: nil, GlobalWrite: &global_write }, self.Username, &globals) 
    if err != nil {
        t.Fatalf("failed to create the project; %v", err)
    }

    t.Run("okay", func(t *testing.T) {
        asset := "BAR"
        version := "whee"
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, filepath.Base(src), project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = uploadHandler(reqname, &globals)
        if err != nil {
            t.Fatalf("failed to perform a global write upload; %v", err)
        }

        // Checking that permissions are set up correctly.
        perms, err := addAssetPermissionsForUpload(&permissionsMetadata{}, filepath.Join(globals.Registry, project, asset), asset)
        if err != nil {
            t.Fatalf("failed to read the new permissions; %v", err)
        }
        if len(perms.Owners) != 1 || perms.Owners[0] != self.Username {
            t.Fatalf("global write upload did not update the uploaders; %v", err)
        }

        // Check that the upload completed.
        destination := filepath.Join(reg, project, asset, version)
        man, err := readManifest(destination)
        info, ok := man["evolution"]
        if !ok || int(info.Size) != len("haunter") || info.Link != nil {
            t.Fatal("unexpected manifest entry for 'evolution'")
        }

        // Checking that the new permissions are acknowledged so that we can add another version of the same asset.
        version = "whee2"
        req_string = fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, filepath.Base(src), project, asset, version)
        reqname, err = dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = uploadHandler(reqname, &globals)
        if err != nil {
            t.Fatalf("failed to perform another upload to the same asset; %v", err)
        }
    })

    t.Run("already there", func(t *testing.T) {
        asset := "FOO"
        err := os.Mkdir(filepath.Join(reg, project, asset), 0755)
        if err != nil {
            t.Fatal(err)
        }

        version := "whee"
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, filepath.Base(src), project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = uploadHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "not authorized") {
            t.Fatal("global write should have failed if asset already exists")
        }
    })
}

func TestUploadHandlerProbation(t *testing.T) {
    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg)

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    project := "setsuna"
    asset := "yuki"
    version := "nakagawa"

    err = setupProjectForUploadTest(project, &globals)
    if err != nil {
        t.Fatalf("failed to set up project directory; %v", err)
    }

    req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s", "on_probation": true }`, filepath.Base(src), project, asset, version)
    reqname, err := dumpRequest("upload", req_string)
    if err != nil {
        t.Fatalf("failed to create upload request; %v", err)
    }

    err = uploadHandler(reqname, &globals)
    if err != nil {
        t.Fatalf("failed to perform the upload; %v", err)
    }

    // Summary file states that it's on probation.
    summ, err := readSummary(filepath.Join(reg, project, asset, version))
    if err != nil {
        t.Fatalf("failed to read the summary; %v", err)
    }
    if !summ.IsProbational() {
        t.Fatal("expected version to be on probation")
    }

    // No latest file should be created for probational projects.
    _, err = readLatest(filepath.Join(reg, project, asset))
    if err == nil || !errors.Is(err, os.ErrNotExist) {
        t.Fatal("no ..latest file should be created on probation")
    }

    // No logs should be created either.
    logs, err := os.ReadDir(filepath.Join(reg, logDirName))
    if err != nil {
        t.Fatalf("failed to read the log directory")
    }
    if len(logs) != 0 {
        t.Fatalf("no logs should be created on probation")
    }
}

func TestUploadHandlerUpdateOnProbation(t *testing.T) {
    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg)

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    self, err := user.Current()
    if err != nil {
        t.Fatalf("could not identify the current user; %v", err)
    }
    self_name := self.Username

    // Uploaders are not trusted by default.
    t.Run("untrusted uploader", func(t *testing.T) {
        project := "ghost"
        err := setupProjectForUploadTestWithPermissions(project, []string{}, []unsafeUploaderEntry{ unsafeUploaderEntry{ Id: &self_name } }, &globals)
        if err != nil {
            t.Fatalf("failed to create the project; %v", err)
        }

        asset := "gastly"
        version := "shadow_ball"
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, filepath.Base(src), project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = uploadHandler(reqname, &globals)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        summ, err := readSummary(filepath.Join(reg, project, asset, version))
        if err != nil {
            t.Fatalf("failed to read the summary file; %v", err)
        }

        if !summ.IsProbational() {
            t.Fatal("expected 'on_probation' to be 'true'")
        }
    })

    // Checking that trusted uploaders do not get probation.
    t.Run("trusted uploader", func(t *testing.T) {
        project := "pokemon_adventures" // changing the project name to get a new project.
        trusted := true
        err := setupProjectForUploadTestWithPermissions(project, []string{}, []unsafeUploaderEntry{ unsafeUploaderEntry{ Id: &self_name, Trusted: &trusted } }, &globals)
        if err != nil {
            t.Fatalf("failed to create the project; %v", err)
        }

        asset := "gastly"
        version := "dream_eater"
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, filepath.Base(src), project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = uploadHandler(reqname, &globals)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        summ, err := readSummary(filepath.Join(reg, project, asset, version))
        if err != nil {
            t.Fatalf("failed to read the summary file; %v", err)
        }

        if summ.OnProbation != nil {
            t.Fatal("expected no 'on_probation' entry to be present")
        }

        // ... unless they specifically ask for it.
        version = "hypnosis"
        req_string = fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s", "on_probation": true }`, filepath.Base(src), project, asset, version)
        reqname, err = dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = uploadHandler(reqname, &globals)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        summ, err = readSummary(filepath.Join(reg, project, asset, version))
        if err != nil {
            t.Fatalf("failed to read the summary file; %v", err)
        }

        if !summ.IsProbational() {
            t.Fatal("expected 'on_probation' to be 'true'")
        }
    })

    // Owners are free from probation.
    t.Run("owner", func(t *testing.T) {
        project := "ss_anne" // changing project name again.
        err := setupProjectForUploadTestWithPermissions(project, []string{ self_name }, nil, &globals)
        if err != nil {
            t.Fatalf("failed to create the project; %v", err)
        }

        asset := "gastly"
        version := "confuse_ray"
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, filepath.Base(src), project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = uploadHandler(reqname, &globals)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        summ, err := readSummary(filepath.Join(reg, project, asset, version))
        if err != nil {
            t.Fatalf("failed to read the summary file; %v", err)
        }

        if summ.OnProbation != nil {
            t.Fatal("expected no 'on_probation' entry to be present")
        }
    })
}

func TestUploadHandlerConsume(t *testing.T) {
    project := "pokemon"
    asset := "gastly"
    version := "lavender"

    quickSetup := func() (string, globalConfiguration, string) {
        reg, err := constructMockRegistry()
        if err != nil {
            t.Fatalf("failed to create the registry; %v", err)
        }
        globals := newGlobalConfiguration(reg)

        err = setupProjectForUploadTest(project, &globals)
        if err != nil {
            t.Fatalf("failed to set up project directory; %v", err)
        }

        src, err := setupSourceForUploadTest()
        if err != nil {
            t.Fatalf("failed to set up test directories; %v", err)
        }

        return reg, globals, src
    }

    t.Run("no consume", func(t *testing.T) {
        _, globals, src := quickSetup()

        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": %q, "consume": false }`, filepath.Base(src), project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = uploadHandler(reqname, &globals)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        err = verifyFileContents(filepath.Join(src, "moves"), "lick,confuse_ray,shadow_ball,dream_eater")
        if err != nil {
            t.Errorf("could not verify 'moves' in the source; %v", err)
        }
    })

    t.Run("with consume", func(t *testing.T) {
        reg, globals, src := quickSetup()

        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": %q, "consume": true }`, filepath.Base(src), project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = uploadHandler(reqname, &globals)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        destination := filepath.Join(reg, project, asset, version)
        err = verifyFileContents(filepath.Join(destination, "moves"), "lick,confuse_ray,shadow_ball,dream_eater")
        if err != nil {
            t.Errorf("could not verify 'moves' in the registry; %v", err)
        }
        if _, err := os.Stat(filepath.Join(src, "moves")); err == nil || !errors.Is(err, os.ErrNotExist) {
            t.Errorf("should have consumed 'moves' in the source; %v", err)
        }
    })
}

func TestUploadHandlerIgnoreDot(t *testing.T) {
    project := "pokemon"
    asset := "gastly"

    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg)

    err = setupProjectForUploadTest(project, &globals)
    if err != nil {
        t.Fatalf("failed to set up project directory; %v", err)
    }

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }
    err = os.WriteFile(filepath.Join(src, ".final"), []byte("gengar"), 0644)
    if err != nil {
        t.Fatalf("failed to write a hidden file; %v", err)
    }

    t.Run("with dots", func(t *testing.T) {
        version := "lavender"
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": %q, "ignore_dot": false }`, filepath.Base(src), project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = uploadHandler(reqname, &globals)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        destination := filepath.Join(reg, project, asset, version)
        err = verifyFileContents(filepath.Join(destination, ".final"), "gengar")
        if err != nil {
            t.Errorf("could not verify '.final' in the registry; %v", err)
        }
    })

    t.Run("without dots", func(t *testing.T) {
        version := "lost_cave"
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": %q, "ignore_dot": true }`, filepath.Base(src), project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = uploadHandler(reqname, &globals)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        destination := filepath.Join(reg, project, asset, version)
        if _, err := os.Stat(filepath.Join(destination, ".final")); err == nil || !errors.Is(err, os.ErrNotExist) {
            t.Errorf("'.final' should not be in the registry; %v", err)
        }
    })
}
