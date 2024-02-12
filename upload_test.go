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

func TestIncrementSeries(t *testing.T) {
    for _, prefix := range []string{ "V", "" } {
        dir, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        candidate, err := incrementSeries(prefix, dir)
        if err != nil {
            t.Fatalf("failed to initialize the series; %v", err)
        }
        if candidate != prefix + "1" {
            t.Fatalf("initial value of the series should be 1, got %s", candidate)
        }

        candidate, err = incrementSeries(prefix, dir)
        if err != nil {
            t.Fatalf("failed to update the series; %v", err)
        }
        if candidate != prefix + "2" {
            t.Fatalf("next value of the series should be 2, not %s", candidate)
        }

        // Works after conflict.
        _, err = os.Create(filepath.Join(dir, prefix + "3"))
        if err != nil {
            t.Fatalf("failed to create a conflicting file")
        }
        candidate, err = incrementSeries(prefix, dir)
        if err != nil {
            t.Fatalf("failed to update the series after conflict; %v", err)
        }
        if candidate != prefix + "4" {
            t.Fatal("next value of the series should be 4")
        }

        // Injecting a different value.
        series_path := incrementSeriesPath(prefix, dir)
        err = os.WriteFile(series_path, []byte("100"), 0644)
        if err != nil {
            t.Fatalf("failed to overwrite the series file")
        }
        candidate, err = incrementSeries(prefix, dir)
        if err != nil {
            t.Fatalf("failed to update the series after overwrite; %v", err)
        }
        if candidate != prefix + "101" {
            t.Fatal("next value of the series should be 101")
        }
    }
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

func TestUploadHandlerSimple(t *testing.T) {
    project := "original_series"
    asset := "gastly"
    version := "lavender"

    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, src, project, asset, version)
    reqname, err := dumpRequest("upload", req_string)
    if err != nil {
        t.Fatalf("failed to create upload request; %v", err)
    }

    // Executing the upload.
    config, err := uploadHandler(reqname, reg, nil)
    if err != nil {
        t.Fatalf("failed to perform the upload; %v", err)
    }
    if config.Project != project {
        t.Fatalf("unexpected project name %q", config.Project)
    }
    if config.Version != version {
        t.Fatalf("unexpected version name %q", config.Version)
    }

    // Checking a few manifest entries and files.
    destination := filepath.Join(reg, config.Project, asset, config.Version)
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
    project_dir := filepath.Join(reg, config.Project)
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
    latest, err := readLatest(filepath.Join(reg, config.Project, asset))
    if err != nil {
        t.Fatalf("failed to read the latest; %v", err)
    }
    if latest.Latest != config.Version {
        t.Fatalf("unexpected latest version (expected %q, got %q)", latest.Latest, config.Version)
    }

    quota_raw, err := os.ReadFile(filepath.Join(reg, config.Project, "..quota"))
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
}

func TestUploadHandlerSimpleFailures(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    {
        project := "test"
        asset := "gastly"
        version := "lavender"

        req_string := fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        _, err = uploadHandler(reqname, reg, nil)
        if err == nil || !strings.Contains(err.Error(), "expected a 'source'") {
            t.Fatalf("configuration should have failed without a source")
        }
    }

    {
        project := "FOO"
        asset := "gastly"
        version := "lavender"

        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, src, project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        _, err = uploadHandler(reqname, reg, nil)
        if err == nil || !strings.Contains(err.Error(), "uppercase") {
            t.Fatal("configuration should fail for upper-cased project names")
        }
    }

    {
        project := "..foo"
        asset := "gastly"
        version := "lavender"

        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, src, project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        _, err = uploadHandler(reqname, reg, nil)
        if err == nil || !strings.Contains(err.Error(), "invalid project name") {
            t.Fatal("configuration should fail for invalid project name")
        }
    }

    {
        project := "foobar"
        asset := "..gastly"
        version := "lavender"

        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, src, project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        _, err = uploadHandler(reqname, reg, nil)
        if err == nil || !strings.Contains(err.Error(), "invalid asset name") {
            t.Fatal("configuration should fail for invalid asset name")
        }
    }

    {
        project := "foobar"
        asset := "gastly"
        version := "..lavender"

        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, src, project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        _, err = uploadHandler(reqname, reg, nil)
        if err == nil || !strings.Contains(err.Error(), "invalid version name") {
            t.Fatal("configuration should fail for invalid version name")
        }
    }
}

func TestUploadHandlerNewPermissions(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    asset := "gastly"
    version := "lavender"

    // Checking that owners are respected.
    {
        project := "indigo_league"
        perm_string := `{ "owners": [ "YAY", "NAY" ] }`

        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s", "permissions": %s }`, src, project, asset, version, perm_string)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        config, err := uploadHandler(reqname, reg, nil)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        perms, err := readPermissions(filepath.Join(reg, config.Project))
        if err != nil {
            t.Fatalf("failed to read the permissions; %v", err)
        }
        if len(perms.Owners) != 2 || perms.Owners[0] != "YAY" || perms.Owners[1] != "NAY" {
            t.Fatal("failed to set the owners correctly in the project permissions")
        }
        if len(perms.Uploaders) != 0 {
            t.Fatal("failed to set the uploaders correctly in the project permissions")
        }
    }

    {
        project := "johto_journeys"
        new_id := "foo"
        perm_string := fmt.Sprintf(`{ "uploaders": [ { "id": "%s" } ] }`, new_id)

        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s", "permissions": %s }`, src, project, asset, version, perm_string)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        config, err := uploadHandler(reqname, reg, nil)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        perms, err := readPermissions(filepath.Join(reg, config.Project))
        if err != nil {
            t.Fatalf("failed to read the permissions; %v", err)
        }
        if len(perms.Owners) != 1 { // switches to the uploading user.
            t.Fatal("failed to set the owners correctly in the project permissions")
        }
        if len(perms.Uploaders) != 1 || *(perms.Uploaders[0].Id) != new_id {
            t.Fatal("failed to set the uploaders correctly in the project permissions")
        }
    }

    // Check that uploaders in the permissions are validated.
    {
        project := "battle_frontier"
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s", "permissions": { "uploaders": [{}] } }`, src, project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        _, err = uploadHandler(reqname, reg, nil)
        if err == nil || !strings.Contains(err.Error(), "invalid 'permissions.uploaders'") {
            t.Fatalf("expected upload to fail from invalid 'uploaders'")
        }
    }

    {
        project := "sinnoh_league"
        perm_string := `{ "uploaders": [ { "id": "argle", "until": "bargle" } ] }`

        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s", "permissions": %s }`, src, project, asset, version, perm_string)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        _, err = uploadHandler(reqname, reg, nil)
        if err == nil || !strings.Contains(err.Error(), "invalid 'permissions.uploaders'") {
            t.Fatalf("expected upload to fail from invalid 'uploaders'")
        }
    }
}

func TestUploadHandlerSimpleUpdate(t *testing.T) {
    project := "original_series"
    asset := "gastly"
    version := "lavender"

    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Uploading the first version.
    old_usage := int64(0)
    {
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, src, project, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        config, err := uploadHandler(reqname, reg, nil)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        used, err := readUsage(filepath.Join(reg, config.Project))
        if err != nil {
            t.Fatalf("failed to read the usage; %v", err)
        }
        old_usage = used.Total
    }

    // Executing another transfer on a different version.
    version = "cerulean"
    {
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

        config, err := uploadHandler(reqname, reg, nil)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }
        if config.Version != version {
            t.Fatalf("unexpected version name %q", config.Version)
        }

        destination := filepath.Join(reg, config.Project, asset, config.Version)
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
        project_dir := filepath.Join(reg, config.Project)
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
        latest, err := readLatest(filepath.Join(reg, config.Project, asset))
        if err != nil {
            t.Fatalf("failed to read the latest; %v", err)
        }
        if latest.Latest != config.Version {
            t.Fatalf("unexpected latest version (expected %q, got %q)", config.Version, latest.Latest)
        }
    }
}

func TestUploadHandlerSimpleUpdateFailures(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Ceating the first version.
    project := "aaron"
    asset := "BAR"
    version := "whee"

    req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s" }`, src, project, asset, version)
    reqname, err := dumpRequest("upload", req_string)
    if err != nil {
        t.Fatalf("failed to create upload request; %v", err)
    }

    _, err = uploadHandler(reqname, reg, nil)
    if err != nil {
        t.Fatalf("failed complete configuration; %v", err)
    }
    if _, err := os.Stat(filepath.Join(reg, project, asset, version)); err != nil {
        t.Fatalf("expected creation of the target version directory")
    }

    // Trying with an existing version.
    _, err = uploadHandler(reqname, reg, nil)
    if err == nil || !strings.Contains(err.Error(), "already exists") {
        t.Fatal("configuration should fail for an existing version")
    }

    // Trying without any version.
    req_string = fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s" }`, src, project, asset)
    reqname, err = dumpRequest("upload", req_string)
    if err != nil {
        t.Fatalf("failed to create upload request; %v", err)
    }

    _, err = uploadHandler(reqname, reg, nil)
    if err == nil || !strings.Contains(err.Error(), "initialized without a version series") {
        t.Fatal("configuration should fail for missing version in a non-series asset")
    }
}

func TestUploadHandlerUpdatePermissions(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // First creating the first version.
    project := "aaron"
    asset := "BAR"
    version := "whee"

    req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "%s", "permissions": { "owners": [] } }`, src, project, asset, version)
    reqname, err := dumpRequest("upload", req_string)
    if err != nil {
        t.Fatalf("failed to create upload request; %v", err)
    }

    _, err = uploadHandler(reqname, reg, nil)
    if err != nil {
        t.Fatalf("failed complete configuration; %v", err)
    }
    if _, err := os.Stat(filepath.Join(reg, project, asset, version)); err != nil {
        t.Fatalf("expected creation of the target version directory")
    }

    // Now attempting to create a new version.
    req_string = fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "stuff" }`, src, project, asset)
    reqname, err = dumpRequest("upload", req_string)
    if err != nil {
        t.Fatalf("failed to create upload request; %v", err)
    }

    _, err = uploadHandler(reqname, reg, nil)
    if err == nil || !strings.Contains(err.Error(), "not authorized") {
        t.Fatalf("failed to reject upload from non-authorized user")
    }
}

func TestUploadHandlerProjectSeries(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    prefix := "FOO"
    asset := "gastly"
    version := "v1"

    req_string := fmt.Sprintf(`{ "source": "%s", "prefix": "%s", "asset": "%s", "version": "%s" }`, src, prefix, asset, version)
    reqname, err := dumpRequest("upload", req_string)
    if err != nil {
        t.Fatalf("failed to create upload request; %v", err)
    }

    config, err := uploadHandler(reqname, reg, nil)
    if err != nil {
        t.Fatalf("failed complete configuration; %v", err)
    }
    if config.Project != "FOO1" {
        t.Fatalf("unexpected value for the project name (%s)", config.Project)
    }

    // Check that everything was created.
    if _, err := os.Stat(filepath.Join(reg, config.Project, "..permissions")); err != nil {
        t.Fatalf("permissions file was not created")
    }
    if _, err := os.Stat(filepath.Join(reg, config.Project, "..usage")); err != nil {
        t.Fatalf("usage file was not created")
    }
    if _, err := os.Stat(filepath.Join(reg, config.Project, "..quota")); err != nil {
        t.Fatalf("quota file was not created")
    }

    // Trying again.
    config, err = uploadHandler(reqname, reg, nil)
    if err != nil {
        t.Fatalf("failed complete configuration; %v", err)
    }
    if config.Project != "FOO2" {
        t.Fatalf("unexpected value for the project name (%s)", config.Project)
    }
}

func TestUploadHandlerProjectSeriesFailures(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    {
        asset := "gastly"
        version := "v1"

        req_string := fmt.Sprintf(`{ "source": "%s", "asset": "%s", "version": "%s" }`, src, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        _, err = uploadHandler(reqname, reg, nil)
        if err == nil || !strings.Contains(err.Error(), "expected a 'prefix'") {
            t.Fatalf("configuration should have failed without a prefix")
        }
    }

    {
        prefix := "foo"
        asset := "gastly"
        version := "v1"

        req_string := fmt.Sprintf(`{ "source": "%s", "prefix": "%s", "asset": "%s", "version": "%s" }`, src, prefix, asset, version)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        _, err = uploadHandler(reqname, reg, nil)
        if err == nil || !strings.Contains(err.Error(), "uppercase") {
            t.Fatalf("configuration should have failed with non-uppercase prefix")
        }
    }
}

func TestUploadHandlerVersionSeries(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // First creating the first version.
    project := "aaron"
    asset := "BAR"

    req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s" }`, src, project, asset)
    reqname, err := dumpRequest("upload", req_string)
    if err != nil {
        t.Fatalf("failed to create upload request; %v", err)
    }

    config, err := uploadHandler(reqname, reg, nil)
    if err != nil {
        t.Fatalf("failed complete configuration; %v", err)
    }
    if config.Version != "1" {
        t.Fatalf("expected version series to start at 1");
    }
    if _, err := os.Stat(filepath.Join(reg, project, asset, config.Version)); err != nil {
        t.Fatalf("expected creation of the first version directory")
    }

    // Trying again.
    config, err = uploadHandler(reqname, reg, nil)
    if err != nil {
        t.Fatalf("failed complete configuration; %v", err)
    }
    if config.Version != "2" {
        t.Fatalf("expected version series to continue to 2");
    }
    if _, err := os.Stat(filepath.Join(reg, project, asset, config.Version)); err != nil {
        t.Fatalf("expected creation of the second version directory")
    }

    // Trying with a version.
    req_string = fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "version": "FOO" }`, src, project, asset)
    reqname, err = dumpRequest("upload", req_string)
    if err != nil {
        t.Fatalf("failed to create upload request; %v", err)
    }

    _, err = uploadHandler(reqname, reg, nil)
    if err == nil || !strings.Contains(err.Error(), "initialized with a version series") {
        t.Fatal("configuration should fail for specified version in an asset with seriesc")
    }
}

func TestUploadHandlerNewOnProbation(t *testing.T) {
    prefix := "POKEDEX"
    asset := "Gastly"

    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    src, err := setupSourceForUploadTest()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    req_string := fmt.Sprintf(`{ "source": "%s", "prefix": "%s", "asset": "%s", "version": "FOO", "on_probation": true }`, src, prefix, asset)
    reqname, err := dumpRequest("upload", req_string)
    if err != nil {
        t.Fatalf("failed to create upload request; %v", err)
    }

    config, err := uploadHandler(reqname, reg, nil)
    if err != nil {
        t.Fatalf("failed to perform the upload; %v", err)
    }
    if config.Project != "POKEDEX1" {
        t.Fatalf("unexpected project name %q", config.Project)
    }
    if config.Version != "FOO" {
        t.Fatalf("unexpected version name %q", config.Version)
    }

    // Summary file states that it's on probation.
    summ, err := readSummary(filepath.Join(reg, config.Project, asset, config.Version))
    if err != nil {
        t.Fatalf("failed to read the summary; %v", err)
    }
    if !summ.IsProbational() {
        t.Fatal("expected version to be on probation")
    }

    // No latest file should be created for probational projects.
    _, err = readLatest(filepath.Join(reg, config.Project, asset))
    if err == nil || !errors.Is(err, os.ErrNotExist) {
        t.Fatal("no ..latest file should be created on probation")
    }
}

func TestUploadHandlerUpdateOnProbation(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

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
        asset := "gastly"
        perm_string := fmt.Sprintf(`{ "owners": [], "uploaders": [ { "id": "%s" } ] }`, self_name)
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "permissions": %s }`, src, project, asset, perm_string)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        // First upload to set up the project.
        config, err := uploadHandler(reqname, reg, nil)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        summ, err := readSummary(filepath.Join(reg, project, asset, config.Version))
        if err != nil {
            t.Fatalf("failed to read the summary file; %v", err)
        }

        if summ.OnProbation != nil {
            t.Fatal("expected no 'on_probation' entry to be present")
        }

        // Second upload using the previous permissions.
        config, err = uploadHandler(reqname, reg, nil)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        summ, err = readSummary(filepath.Join(reg, project, asset, config.Version))
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
        asset := "gastly"
        perm_string := fmt.Sprintf(`{ "owners": [], "uploaders": [ { "id": "%s", "trusted": true } ] }`, self_name)
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "permissions": %s }`, src, project, asset, perm_string)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        // First upload.
        _, err = uploadHandler(reqname, reg, nil)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        // Second upload.
        config, err := uploadHandler(reqname, reg, nil)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        summ, err := readSummary(filepath.Join(reg, project, asset, config.Version))
        if err != nil {
            t.Fatalf("failed to read the summary file; %v", err)
        }

        if summ.OnProbation != nil {
            t.Fatal("expected no 'on_probation' entry to be present")
        }

        // ... unless they specifically ask for it.
        req_string = fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "on_probation": true }`, src, project, asset)
        reqname, err = dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        config, err = uploadHandler(reqname, reg, nil)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        summ, err = readSummary(filepath.Join(reg, project, asset, config.Version))
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
        asset := "gastly"
        perm_string := fmt.Sprintf(`{ "owners": [ "%s" ] }`, self_name)
        req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s", "permissions": %s }`, src, project, asset, perm_string)
        reqname, err := dumpRequest("upload", req_string)
        if err != nil {
            t.Fatalf("failed to create upload request; %v", err)
        }

        // First upload to set up the project.
        _, err = uploadHandler(reqname, reg, nil)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        // Second upload.
        config, err := uploadHandler(reqname, reg, nil)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }

        summ, err := readSummary(filepath.Join(reg, project, asset, config.Version))
        if err != nil {
            t.Fatalf("failed to read the summary file; %v", err)
        }

        if summ.OnProbation != nil {
            t.Fatal("expected no 'on_probation' entry to be present")
        }
    }
}
