package main

import (
    "testing"
    "os"
    "path/filepath"
    "fmt"
    "os/user"
    "time"
    "encoding/json"
    "strings"
    "errors"
)

func TestUploadRequestRegistry(t *testing.T) {
    src := "foobar"
    ureq := &uploadRequest{ Source: &src }
    now := time.Now()

    t.Run("basic", func(t *testing.T) {
        ureg := newUploadRequestRegistry(11)

        tok, err := ureg.Add(ureq, "myself", now)
        if err != nil {
            t.Fatal(err)
        }

        tok2, err := ureg.Add(ureq, "foobar", now)
        if err != nil {
            t.Fatal(err)
        }
        if tok == tok2 {
            t.Fatal("tokens should be unique")
        }

        ureq2, user2, start2 := ureg.Pop(tok)
        if *(ureq2.Source) != src || user2 != "myself" || start2 != now {
            t.Fatalf("unexpected result from popping the token")
        }

        ureq2, _, _ = ureg.Pop(tok)
        if ureq2 != nil {
            t.Fatalf("upload request should have been popped out")
        }

        ureq2, user2, start2 = ureg.Pop(tok2)
        if user2 != "foobar" {
            t.Fatalf("unexpected result from popping the second token")
        }
    })

    t.Run("expired", func(t *testing.T) {
        ureg := newUploadRequestRegistry(11)
        ureg.TokenExpiry = time.Millisecond * 100

        tok, err := ureg.Add(ureq, "myself", now)
        if err != nil {
            t.Fatal(err)
        }

        ureq2, _, _ := ureg.Pop(tok)
        if ureq2 == nil {
            t.Fatalf("upload request should not expired yet")
        }

        time.Sleep(time.Millisecond * 200)
        ureq2, _, _ = ureg.Pop(tok)
        if ureq2 != nil {
            t.Fatalf("upload request should have been expired")
        }
    })

    t.Run("token failure", func(t *testing.T) {
        ureg := newUploadRequestRegistry(11)
        ureg.TokenMaxAttempts = 0
        _, err := ureg.Add(ureq, "myself", now)
        if err == nil || !strings.Contains(err.Error(), "unique token") {
            t.Fatal("expected token acquisition to fail")
        }
    })
}

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

func runFullUpload(reqname string, upreg *uploadRequestRegistry, globals *globalConfiguration, src string, t *testing.T) error {
    token, err := uploadPreflightHandler(reqname, upreg)
    if err != nil {
        t.Fatalf("failed to perform upload preflight; %v", err)
    }
    err = os.WriteFile(filepath.Join(src, ".token"), []byte(token), 0755)
    if err != nil {
        t.Fatalf("failed to write token file; %v", err)
    }
    return uploadHandler(token, upreg, ".token", globals)
}

func TestUploadHandlerSimple(t *testing.T) {
    project := "original_series"
    asset := "gastly"
    version := "lavender"

    // Setting up all the files. 
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

    req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, src, project, asset, version)
    reqname, err := dumpRequest("upload", req_string)
    if err != nil {
        t.Fatalf("failed to create upload request; %v", err)
    }

    // Performing the upload.
    upreg := newUploadRequestRegistry(11)
    err = runFullUpload(reqname, upreg, &globals, src, t)
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
    expected_usage, err := computeUsage(project_dir, true)
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

func TestUploadPreflightHandlerFailures(t *testing.T) {
    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    upreg := newUploadRequestRegistry(11)
    project := "test"
    asset := "gastly"
    version := "lavender"

    t.Run("source", func(t *testing.T) {
        req_string := fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }
        _, err = uploadPreflightHandler(reqname, upreg)
        if err == nil || !strings.Contains(err.Error(), "expected a 'source'") {
            t.Fatalf("configuration should have failed without a source")
        }

        req_string = fmt.Sprintf(`{ "source": "foo", "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version)
        reqname, err = dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }
        _, err = uploadPreflightHandler(reqname, upreg)
        if err == nil || !strings.Contains(err.Error(), "absolute path") {
            t.Fatalf("configuration should have failed without an absolute source path")
        }
    })

    t.Run("project", func(t *testing.T) {
        req_string := fmt.Sprintf(`{ "source": "%s", "asset": "%s", "version": "%s" }`, src, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }
        _, err = uploadPreflightHandler(reqname, upreg)
        if err == nil || !strings.Contains(err.Error(), "expected a 'project'") {
            t.Fatalf("configuration should have failed without a project property")
        }

        req_string = fmt.Sprintf(`{ "source": "%s", "project": "bad/name", "asset": "%s", "version": "%s" }`, src, asset, version)
        reqname, err = dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }
        _, err = uploadPreflightHandler(reqname, upreg)
        if err == nil || !strings.Contains(err.Error(), "invalid project name") {
            t.Fatalf("configuration should have failed with an invalid project name")
        }
    })

    t.Run("asset", func(t *testing.T) {
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "version": "%s" }`, src, project, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }
        _, err = uploadPreflightHandler(reqname, upreg)
        if err == nil || !strings.Contains(err.Error(), "expected a 'asset'") {
            t.Fatalf("configuration should have failed without a asset property")
        }

        req_string = fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "..badname", "version": "%s" }`, src, project, version)
        reqname, err = dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }
        _, err = uploadPreflightHandler(reqname, upreg)
        if err == nil || !strings.Contains(err.Error(), "invalid asset name") {
            t.Fatalf("configuration should have failed with an invalid asset name")
        }
    })

    t.Run("version", func(t *testing.T) {
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s" }`, src, project, asset)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }
        _, err = uploadPreflightHandler(reqname, upreg)
        if err == nil || !strings.Contains(err.Error(), "expected a 'version'") {
            t.Fatalf("configuration should have failed without a version property")
        }

        req_string = fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "" }`, src, project, asset)
        reqname, err = dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }
        _, err = uploadPreflightHandler(reqname, upreg)
        if err == nil || !strings.Contains(err.Error(), "invalid version name") {
            t.Fatalf("configuration should have failed with an invalid version name")
        }
    })
}

func TestUploadHandlerFailures(t *testing.T) {
    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg)

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    upreg := newUploadRequestRegistry(11)
    project := "test"
    asset := "gastly"
    version := "lavender"

    req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, src, project, asset, version)
    reqname, err := dumpRequest("upload", req_string)
    if err != nil {
        t.Fatalf("failed to create upload request; %v", err)
    }

    t.Run("no token", func(t *testing.T) {
        err := uploadHandler("Whee", upreg, ".token", &globals)
        if err == nil || !strings.Contains(err.Error(), "no upload request") {
            t.Fatalf("upload should fail without any existing token")
        }
    })

    t.Run("bad token file", func(t *testing.T) {
        token, err := uploadPreflightHandler(reqname, upreg)
        if err != nil {
            t.Fatalf("failed to perform upload preflight; %v", err)
        }
        err = uploadHandler(token, upreg, "foo/token", &globals)
        if err == nil || !strings.Contains(err.Error(), "should refer to a file") {
            t.Fatalf("upload should fail with invalid token file name")
        }

        token, err = uploadPreflightHandler(reqname, upreg)
        if err != nil {
            t.Fatalf("failed to perform upload preflight; %v", err)
        }
        err = uploadHandler(token, upreg, "foo", &globals)
        if err == nil || !strings.Contains(err.Error(), "should refer to a dotfile") {
            t.Fatalf("upload should fail with invalid token file name")
        }

        token, err = uploadPreflightHandler(reqname, upreg)
        if err != nil {
            t.Fatalf("failed to perform upload preflight; %v", err)
        }
        err = uploadHandler(token, upreg, ".foo", &globals)
        if err == nil || !strings.Contains(err.Error(), "could not read") {
            t.Fatalf("upload should fail with absent token file")
        }

        token, err = uploadPreflightHandler(reqname, upreg)
        if err != nil {
            t.Fatalf("failed to perform upload preflight; %v", err)
        }
        err = os.WriteFile(filepath.Join(src, ".token"), []byte("WHEE"), 0755)
        if err != nil {
            t.Fatalf("failed to write token file; %v", err)
        }
        err = uploadHandler(token, upreg, ".token", &globals)
        if err == nil || !strings.Contains(err.Error(), "not equal") {
            t.Fatalf("upload should fail with incorrect token file")
        }
    })

    t.Run("unauthorized", func(t *testing.T) {
        // Making a project with explicitly no permissions.
        project := "aaron"
        err := setupProjectForUploadTestWithPermissions(project, []string{}, nil, &globals)
        if err != nil {
            t.Fatalf("failed to create the project; %v", err)
        }

        asset := "BAR"
        version := "whee"
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, src, project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = runFullUpload(reqname, upreg, &globals, src, t)
        if err == nil || !strings.Contains(err.Error(), "not authorized") {
            t.Fatalf("failed to reject upload from non-authorized user")
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

    upreg := newUploadRequestRegistry(11)

    // Uploading the first version.
    old_usage := int64(0)
    {
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, src, project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = runFullUpload(reqname, upreg, &globals, src, t)
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
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, src, project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        all_evos := "haunter,gengar"
        err = os.WriteFile(filepath.Join(src, "evolution"), []byte(all_evos), 0644)
        if err != nil {
            t.Fatalf("failed to update the 'evolution' file; %v", err)
        }

        err = runFullUpload(reqname, upreg, &globals, src, t)
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
        expected_usage, err := computeUsage(project_dir, true)
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

    t.Run("existing version", func(t *testing.T) {
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, src, project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = runFullUpload(reqname, upreg, &globals, src, t)
        if err == nil || !strings.Contains(err.Error(), "already exists") {
            t.Fatal("configuration should fail for an existing version")
        }
    })
}

func TestUploadHandlerNewOnProbation(t *testing.T) {
    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg)

    upreg := newUploadRequestRegistry(11)

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

    req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s", "on_probation": true }`, src, project, asset, version)
    reqname, err := dumpRequest("upload", req_string)
    if err != nil {
        t.Fatalf("failed to create upload request; %v", err)
    }

    err = runFullUpload(reqname, upreg, &globals, src, t)
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
    upreg := newUploadRequestRegistry(11)

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
    {
        project := "ghost"
        err := setupProjectForUploadTestWithPermissions(project, []string{}, []unsafeUploaderEntry{ unsafeUploaderEntry{ Id: &self_name } }, &globals)
        if err != nil {
            t.Fatalf("failed to create the project; %v", err)
        }

        asset := "gastly"
        version := "shadow_ball"
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, src, project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = runFullUpload(reqname, upreg, &globals, src, t)
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
    }

    // Checking that trusted uploaders do not get probation.
    {
        project := "pokemon_adventures" // changing the project name to get a new project.
        trusted := true
        err := setupProjectForUploadTestWithPermissions(project, []string{}, []unsafeUploaderEntry{ unsafeUploaderEntry{ Id: &self_name, Trusted: &trusted } }, &globals)
        if err != nil {
            t.Fatalf("failed to create the project; %v", err)
        }

        asset := "gastly"
        version := "dream_eater"
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, src, project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = runFullUpload(reqname, upreg, &globals, src, t)
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
        req_string = fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s", "on_probation": true }`, src, project, asset, version)
        reqname, err = dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = runFullUpload(reqname, upreg, &globals, src, t)
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
    }

    // Owners are free from probation.
    {
        project := "ss_anne" // changing project name again.
        err := setupProjectForUploadTestWithPermissions(project, []string{ self_name }, nil, &globals)
        if err != nil {
            t.Fatalf("failed to create the project; %v", err)
        }

        asset := "gastly"
        version := "confuse_ray"
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, src, project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        err = runFullUpload(reqname, upreg, &globals, src, t)
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
    }
}
