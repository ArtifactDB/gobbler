package main

//func TestConfigureNewProjectOnProbation(t *testing.T) {
//    registry, src, err := setupForConfigureTest()
//    if err != nil {
//        t.Fatal(err)
//    }
//
//    self, err := user.Current()
//    if err != nil {
//        t.Fatalf("could not identify the current user; %v", err)
//    }
//    self_name := self.Username
//
//    project_name := "foo"
//    asset_name := "BAR"
//    req := UploadRequest {
//        Self: "blah",
//        Source: &src,
//        Project: &project_name,
//        Asset: &asset_name,
//        Permissions: &permissionsMetadata {
//            Owners: []string{},
//            Uploaders: []uploaderEntry{ uploaderEntry{ Id: &self_name } },
//        },
//    }
//
//    // Checking that uploaders are not trusted by default.
//    {
//        // Setting up an initial project.
//        _, err := configure(&req, registry, nil)
//        if err != nil {
//            t.Fatalf("failed first pass configuration; %v", err)
//        }
//
//        // Performing a new version request.
//        config, err := configure(&req, registry, nil)
//        if err != nil {
//            t.Fatalf("failed second pass configuration; %v", err)
//        }
//        if !config.OnProbation {
//            t.Fatal("second pass configuration should be on probation for an untrusted user")
//        }
//    }
//
//    // Checking that trusted uploaders do not get probation.
//    project_name = "whee" // changing the project name to get a new project.
//    has_trust := true
//    req.Permissions.Uploaders[0].Trusted = &has_trust;
//    {
//        _, err := configure(&req, registry, nil)
//        if err != nil {
//            t.Fatalf("failed first pass configuration; %v", err)
//        }
//
//        config, err := configure(&req, registry, nil)
//        if err != nil {
//            t.Fatalf("failed second pass configuration; %v", err)
//        }
//        if config.OnProbation {
//            t.Fatal("second pass configuration should not be on probation for a trusted user")
//        }
//    }
//
//    // ... unless they specifically ask for it.
//    project_name = "stuff"
//    is_probation := true
//    req.OnProbation = &is_probation
//    {
//        _, err := configure(&req, registry, nil)
//        if err != nil {
//            t.Fatalf("failed first pass configuration; %v", err)
//        }
//
//        config, err := configure(&req, registry, nil)
//        if err != nil {
//            t.Fatalf("failed second pass configuration; %v", err)
//        }
//        if !config.OnProbation {
//            t.Fatal("second pass configuration should be on probation if requested")
//        }
//    }
//
//    // Owners are also free from probation.
//    project_name = "bar"
//    req.Permissions.Owners = append(req.Permissions.Owners, self_name)
//    req.OnProbation = nil
//    {
//        _, err := configure(&req, registry, nil)
//        if err != nil {
//            t.Fatalf("failed first pass configuration; %v", err)
//        }
//
//        config, err := configure(&req, registry, nil)
//        if err != nil {
//            t.Fatalf("failed second pass configuration; %v", err)
//        }
//        if config.OnProbation {
//            t.Fatal("second pass configuration should not be on probation for owners")
//        }
//    }
//}
//
//func TestConfigureNewProjectBasicFailures(t *testing.T) {
//    project_name := "foo"
//    asset_name := "BAR"
//    version_name := "whee"
//
//    {
//        registry, src, err := setupForConfigureTest()
//        if err != nil {
//            t.Fatal(err)
//        }
//
//        project_name := "FOO"
//        req := UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name, Version: &version_name }
//
//        _, err = configure(&req, registry, nil)
//        if err == nil || !strings.Contains(err.Error(), "uppercase") {
//            t.Fatal("configuration should fail for upper-cased project names")
//        }
//    }
//
//    {
//        registry, src, err := setupForConfigureTest()
//        if err != nil {
//            t.Fatal(err)
//        }
//
//        project_name := "..foo"
//        req := UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name, Version: &version_name }
//        _, err = configure(&req, registry, nil)
//        if err == nil || !strings.Contains(err.Error(), "invalid project name") {
//            t.Fatal("configuration should fail for invalid project name")
//        }
//    }
//
//    {
//        registry, src, err := setupForConfigureTest()
//        if err != nil {
//            t.Fatal(err)
//        }
//
//        asset_name := "..BAR"
//        req := UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name, Version: &version_name }
//        _, err = configure(&req, registry, nil)
//        if err == nil || !strings.Contains(err.Error(), "invalid asset name") {
//            t.Fatal("configuration should fail for invalid asset name")
//        }
//    }
//
//    {
//        registry, src, err := setupForConfigureTest()
//        if err != nil {
//            t.Fatal(err)
//        }
//
//        version_name := "..whee"
//        req := UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name, Version: &version_name }
//        _, err = configure(&req, registry, nil)
//        if err == nil || !strings.Contains(err.Error(), "invalid version name") {
//            t.Fatal("configuration should fail for invalid version name")
//        }
//    }
//}
//
//func TestConfigureNewProjectSeries(t *testing.T) {
//    registry, src, err := setupForConfigureTest()
//    if err != nil {
//        t.Fatal(err)
//    }
//
//    prefix := "FOO"
//    asset_name := "BAR"
//    version_name := "whee"
//    req := UploadRequest { Self: "blah", Source: &src, Prefix: &prefix, Asset: &asset_name, Version: &version_name }
//
//    config, err := configure(&req, registry, nil)
//    if err != nil {
//        t.Fatalf("failed complete configuration; %v", err)
//    }
//    if config.Project != "FOO1" {
//        t.Fatalf("unexpected value for the project name (%s)", config.Project)
//    }
//
//    // Check that everything was created.
//    if _, err := os.Stat(filepath.Join(registry, config.Project, "..permissions")); err != nil {
//        t.Fatalf("permissions file was not created")
//    }
//    if _, err := os.Stat(filepath.Join(registry, config.Project, "..usage")); err != nil {
//        t.Fatalf("usage file was not created")
//    }
//    if _, err := os.Stat(filepath.Join(registry, config.Project, "..quota")); err != nil {
//        t.Fatalf("quota file was not created")
//    }
//
//    config, err = configure(&req, registry, nil)
//    if err != nil {
//        t.Fatalf("failed complete configuration; %v", err)
//    }
//    if config.Project != "FOO2" {
//        t.Fatalf("unexpected value for the project name (%s)", config.Project)
//    }
//}
//
//func TestConfigureNewProjectSeriesFailures(t *testing.T) {
//    registry, src, err := setupForConfigureTest()
//    if err != nil {
//        t.Fatal(err)
//    }
//
//    req := UploadRequest { Self: "blah" }
//    _, err = configure(&req, registry, nil)
//    if err == nil || !strings.Contains(err.Error(), "expected a 'source'") {
//        t.Fatalf("configuration should have failed without a source")
//    }
//
//    asset_name := "BAR"
//    version_name := "whee"
//    req = UploadRequest { Self: "blah", Source: &src, Asset: &asset_name, Version: &version_name }
//    _, err = configure(&req, registry, nil)
//    if err == nil || !strings.Contains(err.Error(), "expected a 'prefix'") {
//        t.Fatalf("configuration should have failed without a prefix")
//    }
//
//    prefix := "foo"
//    req = UploadRequest { Self: "blah", Source: &src, Prefix: &prefix, Asset: &asset_name, Version: &version_name }
//    _, err = configure(&req, registry, nil)
//    if err == nil || !strings.Contains(err.Error(), "only uppercase") {
//        t.Fatalf("configuration should have failed with non-uppercase prefix")
//    }
//}
//
//func TestConfigureUpdateAsset(t *testing.T) {
//    registry, src, err := setupForConfigureTest()
//    if err != nil {
//        t.Fatal(err)
//    }
//
//    // First creating the first version.
//    project_name := "aaron"
//    asset_name := "BAR"
//    version_name := "whee"
//    req := UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name, Version: &version_name }
//
//    _, err = configure(&req, registry, nil)
//    if err != nil {
//        t.Fatalf("failed complete configuration; %v", err)
//    }
//    if _, err := os.Stat(filepath.Join(registry, "aaron", "BAR", "whee")); err != nil {
//        t.Fatalf("expected creation of the target version directory")
//    }
//
//    // Trying with an existing version.
//    _, err = configure(&req, registry, nil)
//    if err == nil || !strings.Contains(err.Error(), "already exists") {
//        t.Fatal("configuration should fail for an existing version")
//    }
//
//    // Updating with a new version.
//    version_name = "stuff"
//    req = UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name, Version: &version_name }
//    _, err = configure(&req, registry, nil)
//    if err != nil {
//        t.Fatalf("failed complete configuration; %v", err)
//    }
//    if _, err := os.Stat(filepath.Join(registry, "aaron", "BAR", "stuff")); err != nil {
//        t.Fatalf("expected creation of the target version directory")
//    }
//
//    // Trying without any version.
//    req = UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name }
//    _, err = configure(&req, registry, nil)
//    if err == nil || !strings.Contains(err.Error(), "initialized without a version series") {
//        t.Fatal("configuration should fail for missing version in a non-series asset")
//    }
//}
//
//func TestConfigureUpdateAssetPermissions(t *testing.T) {
//    registry, src, err := setupForConfigureTest()
//    if err != nil {
//        t.Fatal(err)
//    }
//
//    // First creating the first version.
//    project_name := "aaron"
//    asset_name := "BAR"
//    version_name := "whee"
//    req := UploadRequest {
//        Self: "blah",
//        Source: &src,
//        Project: &project_name,
//        Asset: &asset_name,
//        Version: &version_name,
//        Permissions: &permissionsMetadata{
//            Owners: []string{},
//        },
//    }
//
//    _, err = configure(&req, registry, nil)
//    if err != nil {
//        t.Fatalf("failed complete configuration; %v", err)
//    }
//    if _, err := os.Stat(filepath.Join(registry, "aaron", "BAR", "whee")); err != nil {
//        t.Fatalf("expected creation of the target version directory")
//    }
//
//    // Now attempting to create a new version.
//    version_name = "stuff"
//    _, err = configure(&req, registry, nil)
//    if err == nil || !strings.Contains(err.Error(), "not authorized") {
//        t.Fatalf("failed to reject upload from non-authorized user")
//    }
//}
//
//func TestConfigureUpdateAssetSeries(t *testing.T) {
//    registry, src, err := setupForConfigureTest()
//    if err != nil {
//        t.Fatal(err)
//    }
//
//    // First creating the first version.
//    project_name := "aaron"
//    asset_name := "BAR"
//    req := UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name }
//
//    config, err := configure(&req, registry, nil)
//    if err != nil {
//        t.Fatalf("failed complete configuration; %v", err)
//    }
//    if config.Version != "1" {
//        t.Fatalf("expected version series to start at 1");
//    }
//    if _, err := os.Stat(filepath.Join(registry, "aaron", "BAR", "1")); err != nil {
//        t.Fatalf("expected creation of the first version directory")
//    }
//
//    // Trying again.
//    config, err = configure(&req, registry, nil)
//    if err != nil {
//        t.Fatalf("failed complete configuration; %v", err)
//    }
//    if config.Version != "2" {
//        t.Fatalf("expected version series to continue to 2");
//    }
//    if _, err := os.Stat(filepath.Join(registry, "aaron", "BAR", "2")); err != nil {
//        t.Fatalf("expected creation of the second version directory")
//    }
//
//    // Trying with a version.
//    version_name := "foo"
//    req = UploadRequest { Self: "blah", Source: &src, Project: &project_name, Asset: &asset_name, Version: &version_name }
//    _, err = configure(&req, registry, nil)
//    if err == nil || !strings.Contains(err.Error(), "initialized with a version series") {
//        t.Fatal("configuration should fail for specified version in an asset with seriesc")
//    }
//}
