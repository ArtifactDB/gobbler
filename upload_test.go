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

func TestUploadHandlerPermissions(t *testing.T) {
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

func TestUploadHandlerProbation(t *testing.T) {
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
