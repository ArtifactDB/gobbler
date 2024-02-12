package main

import (
    "testing"
    "io/ioutil"
    "path/filepath"
    "os"
    "strings"
    "fmt"
    "encoding/json"
    "os/user"
)

func TestIncrementSeries(t *testing.T) {
    for _, prefix := range []string{ "V", "" } {
        dir, err := ioutil.TempDir("", "")
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

func setupForConfigureTest() (string, string, error) {
    reg, err := ioutil.TempDir("", "")
    if err != nil {
        return "", "", fmt.Errorf("failed to create the registry; %w", err)
    }

    dir, err := ioutil.TempDir("", "")
    if err != nil {
        return "", "", fmt.Errorf("failed to create the temporary directory; %w", err)
    }

    return reg, dir, nil
}

func TestConfigureNewProjectBasic(t *testing.T) {
    registry, src, err := setupForConfigureTest()
    if err != nil {
        t.Fatal(err)
    }

    project_name := "foo"
    asset_name := "BAR"
    version_name := "whee"
    req := UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name, Version: &version_name }

    config, err := configure(&req, registry, nil)
    if err != nil {
        t.Fatalf("failed complete configuration; %v", err)
    }

    if config.Project != project_name {
        t.Fatalf("unexpected value for the project name (%s)", config.Project)
    }

    if config.Asset != asset_name {
        t.Fatalf("unexpected value for the asset name (%s)", config.Asset)
    }

    if config.Version != version_name {
        t.Fatalf("unexpected value for the version name (%s)", config.Version)
    }

    // Checking the various bits and pieces.
    {
        deets, err := readPermissions(filepath.Join(registry, config.Project))
        if err != nil {
            t.Fatalf("failed to read the permissions; %v", err)
        }
        self, err := user.Current()
        if err != nil {
            t.Fatalf("failed to get the current user; %v", err)
        }
        if len(deets.Owners) != 1 || deets.Owners[0] != self.Username {
            t.Fatalf("expected the current user in the set of permissions")
        }
    }

    {
        usage_raw, err := os.ReadFile(filepath.Join(registry, config.Project, "..usage"))
        if err != nil {
            t.Fatalf("failed to read the usage; %v", err)
        }
        deets := struct { Total int `json:"total"` }{ Total: 100 }
        err = json.Unmarshal(usage_raw, &deets)
        if err != nil {
            t.Fatalf("failed to parse the usage; %v", err)
        }
        if deets.Total != 0 {
            t.Fatalf("expected the total to be zero")
        }
    }

    {
        quota_raw, err := os.ReadFile(filepath.Join(registry, config.Project, "..quota"))
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
}

func TestConfigureNewProjectPermissions(t *testing.T) {
    registry, src, err := setupForConfigureTest()
    if err != nil {
        t.Fatal(err)
    }

    // Check that existing owners in the permissions are respected.
    project_name := "foo"
    asset_name := "BAR"
    req := UploadRequest {
        Self: "blah",
        Source: &src,
        Project: &project_name,
        Asset: &asset_name,
        Permissions: &permissionsMetadata {
            Owners: []string{ "YAY", "NAY"},
        },
    }

    {
        config, err := configure(&req, registry, nil)
        if err != nil {
            t.Fatalf("failed complete configuration; %v", err)
        }

        perms, err := readPermissions(filepath.Join(registry, config.Project))
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

    // Check that uploaders in the permissions are respected.
    project_name = "bar" // changing the name to avoid the permissions of the existing project. 
    req.Permissions.Owners = nil
    new_id := "foo"
    req.Permissions.Uploaders = append(req.Permissions.Uploaders, uploaderEntry{ Id: &new_id })

    {
        config, err := configure(&req, registry, nil)
        if err != nil {
            t.Fatalf("failed complete configuration; %v", err)
        }

        perms, err := readPermissions(filepath.Join(registry, config.Project))
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
    project_name = "stuff"
    req.Permissions.Owners = nil
    req.Permissions.Uploaders[0].Id = nil
    _, err = configure(&req, registry, nil)
    if err == nil || !strings.Contains(err.Error(), "invalid 'permissions.uploaders'") {
        t.Fatalf("failed complete configuration; %v", err)
    }

    project_name = "whee"
    req.Permissions.Owners = nil
    req.Permissions.Uploaders[0].Id = &new_id
    req.Permissions.Uploaders[0].Until = &new_id
    _, err = configure(&req, registry, nil)
    if err == nil || !strings.Contains(err.Error(), "invalid 'permissions.uploaders'") {
        t.Fatalf("failed complete configuration; %v", err)
    }
}

func TestConfigureNewProjectOnProbation(t *testing.T) {
    registry, src, err := setupForConfigureTest()
    if err != nil {
        t.Fatal(err)
    }

    self, err := user.Current()
    if err != nil {
        t.Fatalf("could not identify the current user; %v", err)
    }
    self_name := self.Username

    project_name := "foo"
    asset_name := "BAR"
    req := UploadRequest {
        Self: "blah",
        Source: &src,
        Project: &project_name,
        Asset: &asset_name,
        Permissions: &permissionsMetadata {
            Owners: []string{},
            Uploaders: []uploaderEntry{ uploaderEntry{ Id: &self_name } },
        },
    }

    // Checking that uploaders are not trusted by default.
    {
        // Setting up an initial project.
        _, err := configure(&req, registry, nil)
        if err != nil {
            t.Fatalf("failed first pass configuration; %v", err)
        }

        // Performing a new version request.
        config, err := configure(&req, registry, nil)
        if err != nil {
            t.Fatalf("failed second pass configuration; %v", err)
        }
        if !config.OnProbation {
            t.Fatal("second pass configuration should be on probation for an untrusted user")
        }
    }

    // Checking that trusted uploaders do not get probation.
    project_name = "whee" // changing the project name to get a new project.
    has_trust := true
    req.Permissions.Uploaders[0].Trusted = &has_trust;
    {
        _, err := configure(&req, registry, nil)
        if err != nil {
            t.Fatalf("failed first pass configuration; %v", err)
        }

        config, err := configure(&req, registry, nil)
        if err != nil {
            t.Fatalf("failed second pass configuration; %v", err)
        }
        if config.OnProbation {
            t.Fatal("second pass configuration should not be on probation for a trusted user")
        }
    }

    // ... unless they specifically ask for it.
    project_name = "stuff"
    is_probation := true
    req.OnProbation = &is_probation
    {
        _, err := configure(&req, registry, nil)
        if err != nil {
            t.Fatalf("failed first pass configuration; %v", err)
        }

        config, err := configure(&req, registry, nil)
        if err != nil {
            t.Fatalf("failed second pass configuration; %v", err)
        }
        if !config.OnProbation {
            t.Fatal("second pass configuration should be on probation if requested")
        }
    }

    // Owners are also free from probation.
    project_name = "bar"
    req.Permissions.Owners = append(req.Permissions.Owners, self_name)
    req.OnProbation = nil
    {
        _, err := configure(&req, registry, nil)
        if err != nil {
            t.Fatalf("failed first pass configuration; %v", err)
        }

        config, err := configure(&req, registry, nil)
        if err != nil {
            t.Fatalf("failed second pass configuration; %v", err)
        }
        if config.OnProbation {
            t.Fatal("second pass configuration should not be on probation for owners")
        }
    }
}

func TestConfigureNewProjectBasicFailures(t *testing.T) {
    project_name := "foo"
    asset_name := "BAR"
    version_name := "whee"

    {
        registry, src, err := setupForConfigureTest()
        if err != nil {
            t.Fatal(err)
        }

        project_name := "FOO"
        req := UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name, Version: &version_name }

        _, err = configure(&req, registry, nil)
        if err == nil || !strings.Contains(err.Error(), "uppercase") {
            t.Fatal("configuration should fail for upper-cased project names")
        }
    }

    {
        registry, src, err := setupForConfigureTest()
        if err != nil {
            t.Fatal(err)
        }

        project_name := "..foo"
        req := UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name, Version: &version_name }
        _, err = configure(&req, registry, nil)
        if err == nil || !strings.Contains(err.Error(), "invalid project name") {
            t.Fatal("configuration should fail for invalid project name")
        }
    }

    {
        registry, src, err := setupForConfigureTest()
        if err != nil {
            t.Fatal(err)
        }

        asset_name := "..BAR"
        req := UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name, Version: &version_name }
        _, err = configure(&req, registry, nil)
        if err == nil || !strings.Contains(err.Error(), "invalid asset name") {
            t.Fatal("configuration should fail for invalid asset name")
        }
    }

    {
        registry, src, err := setupForConfigureTest()
        if err != nil {
            t.Fatal(err)
        }

        version_name := "..whee"
        req := UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name, Version: &version_name }
        _, err = configure(&req, registry, nil)
        if err == nil || !strings.Contains(err.Error(), "invalid version name") {
            t.Fatal("configuration should fail for invalid version name")
        }
    }
}

func TestConfigureNewProjectSeries(t *testing.T) {
    registry, src, err := setupForConfigureTest()
    if err != nil {
        t.Fatal(err)
    }

    prefix := "FOO"
    asset_name := "BAR"
    version_name := "whee"
    req := UploadRequest { Self: "blah", Source: &src, Prefix: &prefix, Asset: &asset_name, Version: &version_name }

    config, err := configure(&req, registry, nil)
    if err != nil {
        t.Fatalf("failed complete configuration; %v", err)
    }
    if config.Project != "FOO1" {
        t.Fatalf("unexpected value for the project name (%s)", config.Project)
    }

    // Check that everything was created.
    if _, err := os.Stat(filepath.Join(registry, config.Project, "..permissions")); err != nil {
        t.Fatalf("permissions file was not created")
    }
    if _, err := os.Stat(filepath.Join(registry, config.Project, "..usage")); err != nil {
        t.Fatalf("usage file was not created")
    }
    if _, err := os.Stat(filepath.Join(registry, config.Project, "..quota")); err != nil {
        t.Fatalf("quota file was not created")
    }

    config, err = configure(&req, registry, nil)
    if err != nil {
        t.Fatalf("failed complete configuration; %v", err)
    }
    if config.Project != "FOO2" {
        t.Fatalf("unexpected value for the project name (%s)", config.Project)
    }
}

func TestConfigureNewProjectSeriesFailures(t *testing.T) {
    registry, src, err := setupForConfigureTest()
    if err != nil {
        t.Fatal(err)
    }

    req := UploadRequest { Self: "blah" }
    _, err = configure(&req, registry, nil)
    if err == nil || !strings.Contains(err.Error(), "expected a 'source'") {
        t.Fatalf("configuration should have failed without a source")
    }

    asset_name := "BAR"
    version_name := "whee"
    req = UploadRequest { Self: "blah", Source: &src, Asset: &asset_name, Version: &version_name }
    _, err = configure(&req, registry, nil)
    if err == nil || !strings.Contains(err.Error(), "expected a 'prefix'") {
        t.Fatalf("configuration should have failed without a prefix")
    }

    prefix := "foo"
    req = UploadRequest { Self: "blah", Source: &src, Prefix: &prefix, Asset: &asset_name, Version: &version_name }
    _, err = configure(&req, registry, nil)
    if err == nil || !strings.Contains(err.Error(), "only uppercase") {
        t.Fatalf("configuration should have failed with non-uppercase prefix")
    }
}

func TestConfigureUpdateAsset(t *testing.T) {
    registry, src, err := setupForConfigureTest()
    if err != nil {
        t.Fatal(err)
    }

    // First creating the first version.
    project_name := "aaron"
    asset_name := "BAR"
    version_name := "whee"
    req := UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name, Version: &version_name }

    _, err = configure(&req, registry, nil)
    if err != nil {
        t.Fatalf("failed complete configuration; %v", err)
    }
    if _, err := os.Stat(filepath.Join(registry, "aaron", "BAR", "whee")); err != nil {
        t.Fatalf("expected creation of the target version directory")
    }

    // Trying with an existing version.
    _, err = configure(&req, registry, nil)
    if err == nil || !strings.Contains(err.Error(), "already exists") {
        t.Fatal("configuration should fail for an existing version")
    }

    // Updating with a new version.
    version_name = "stuff"
    req = UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name, Version: &version_name }
    _, err = configure(&req, registry, nil)
    if err != nil {
        t.Fatalf("failed complete configuration; %v", err)
    }
    if _, err := os.Stat(filepath.Join(registry, "aaron", "BAR", "stuff")); err != nil {
        t.Fatalf("expected creation of the target version directory")
    }

    // Trying without any version.
    req = UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name }
    _, err = configure(&req, registry, nil)
    if err == nil || !strings.Contains(err.Error(), "initialized without a version series") {
        t.Fatal("configuration should fail for missing version in a non-series asset")
    }
}

func TestConfigureUpdateAssetPermissions(t *testing.T) {
    registry, src, err := setupForConfigureTest()
    if err != nil {
        t.Fatal(err)
    }

    // First creating the first version.
    project_name := "aaron"
    asset_name := "BAR"
    version_name := "whee"
    req := UploadRequest {
        Self: "blah",
        Source: &src,
        Project: &project_name,
        Asset: &asset_name,
        Version: &version_name,
        Permissions: &permissionsMetadata{
            Owners: []string{},
        },
    }

    _, err = configure(&req, registry, nil)
    if err != nil {
        t.Fatalf("failed complete configuration; %v", err)
    }
    if _, err := os.Stat(filepath.Join(registry, "aaron", "BAR", "whee")); err != nil {
        t.Fatalf("expected creation of the target version directory")
    }

    // Now attempting to create a new version.
    version_name = "stuff"
    _, err = configure(&req, registry, nil)
    if err == nil || !strings.Contains(err.Error(), "not authorized") {
        t.Fatalf("failed to reject upload from non-authorized user")
    }
}

func TestConfigureUpdateAssetSeries(t *testing.T) {
    registry, src, err := setupForConfigureTest()
    if err != nil {
        t.Fatal(err)
    }

    // First creating the first version.
    project_name := "aaron"
    asset_name := "BAR"
    req := UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name }

    config, err := configure(&req, registry, nil)
    if err != nil {
        t.Fatalf("failed complete configuration; %v", err)
    }
    if config.Version != "1" {
        t.Fatalf("expected version series to start at 1");
    }
    if _, err := os.Stat(filepath.Join(registry, "aaron", "BAR", "1")); err != nil {
        t.Fatalf("expected creation of the first version directory")
    }

    // Trying again.
    config, err = configure(&req, registry, nil)
    if err != nil {
        t.Fatalf("failed complete configuration; %v", err)
    }
    if config.Version != "2" {
        t.Fatalf("expected version series to continue to 2");
    }
    if _, err := os.Stat(filepath.Join(registry, "aaron", "BAR", "2")); err != nil {
        t.Fatalf("expected creation of the second version directory")
    }

    // Trying with a version.
    version_name := "foo"
    req = UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name, Version: &version_name }
    _, err = configure(&req, registry, nil)
    if err == nil || !strings.Contains(err.Error(), "initialized with a version series") {
        t.Fatal("configuration should fail for specified version in an asset with seriesc")
    }
}
