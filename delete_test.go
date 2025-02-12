package main

import (
    "testing"
    "os"
    "path/filepath"
    "fmt"
    "strings"
    "time"
    "errors"
)

func mockRegistryForDeletion(project, asset string, versions []string) (string, error) {
    reg, err := constructMockRegistry()
    if err != nil {
        return "", fmt.Errorf("failed to create a registry; %w", err)
    }

    project_dir := filepath.Join(reg, project)
    err = os.Mkdir(project_dir, 0755)
    if err != nil {
        return "", fmt.Errorf("failed to create a mock project; %w", err)
    }

    asset_dir := filepath.Join(project_dir, asset)
    err = os.Mkdir(asset_dir, 0755)
    if err != nil {
        return "", fmt.Errorf("failed to create an asset; %w", err)
    }

    expected_size := 0
    for i, v := range versions {
        version_dir := filepath.Join(asset_dir, v)
        err = os.Mkdir(version_dir, 0755)
        if err != nil {
            return "", fmt.Errorf("failed to create a version; %w", err)
        }

        summ_path := filepath.Join(version_dir, summaryFileName)
        summary := summaryMetadata {
            UploadUserId: "urmom",
            UploadStart: "1999-09-19T19:09:19Z",
            UploadFinish: time.Now().Add(time.Duration(i) * time.Hour).Format(time.RFC3339),
        }
        err = dumpJson(summ_path, &summary)
        if err != nil {
            return "", fmt.Errorf("failed to dump the version summary; %w", err)
        }

        message := "Hi I am version " + v
        err = os.WriteFile(filepath.Join(version_dir, "message.txt"), []byte(message), 0644)
        if err != nil {
            return "", fmt.Errorf("failed to write a placeholder file; %w", err)
        }
        expected_size += len(message)

        err = reindexDirectory(reg, project, asset, v, []string{})
        if err != nil {
            return "", fmt.Errorf("failed to reindex the directory; %w", err)
        }
    }

    err = os.WriteFile(filepath.Join(project_dir, usageFileName), []byte(fmt.Sprintf(`{ "total": %d }`, expected_size)), 0644)
    if err != nil {
        return "", fmt.Errorf("failed to write usage; %w", err)
    }

    err = os.WriteFile(filepath.Join(asset_dir, latestFileName), []byte(fmt.Sprintf(`{ "version": "%s" }`, versions[len(versions) - 1])), 0644)
    if err != nil {
        return "", fmt.Errorf("failed to write latest version; %w", err)
    }

    return reg, nil
}

func TestDeleteProject(t *testing.T) {
    project := "foobar"
    reg, err := mockRegistryForDeletion(project, "stuff", []string{ "1" })
    if err != nil {
        t.Fatalf("failed to mock up registry; %v", err) 
    }

    reqpath, err := dumpRequest("delete_project", fmt.Sprintf(`{ "project": "%s" }`, project))
    if err != nil {
        t.Fatalf("failed to dump a request type; %v", err)
    }

    globals := newGlobalConfiguration(reg)
    err = deleteProjectHandler(reqpath, &globals)
    if err == nil || !strings.Contains(err.Error(), "not authorized") {
        t.Fatal("unexpected authorization for non-admin")
    }

    self, err := identifyUser(reg)
    if err != nil {
        t.Fatalf("failed to identify self; %v", err)
    }
    globals.Administrators = append(globals.Administrators, self)
    err = deleteProjectHandler(reqpath, &globals)
    if err != nil {
        t.Fatalf("failed to delete a project; %v", err)
    }

    project_dir := filepath.Join(reg, project)
    if _, err := os.Stat(project_dir); !errors.Is(err, os.ErrNotExist) {
        t.Fatal("failed to delete the project directory")
    }
    if _, err := os.Stat(reg); err != nil {
        t.Fatal("oops, deleted the entire registry")
    }

    // No-ops if repeated with already-deleted project.
    err = deleteProjectHandler(reqpath, &globals)
    if err != nil {
        t.Fatalf("failed to delete a project; %v", err)
    }

    // Checking that inputs are valid.
    reqpath, err = dumpRequest("delete_project", "{}")
    if err != nil {
        t.Fatalf("failed to dump a request type; %v", err)
    }

    err = deleteProjectHandler(reqpath, &globals)
    if err == nil || !strings.Contains(err.Error(), "invalid 'project'") {
        t.Fatal("fail to throw for invalid request")
    }

    // Checking that logs were correctly written.
    logs, err := readAllLogs(reg)
    if err != nil {
        t.Fatalf("failed to read all logs; %v", err)
    }

    if len(logs) != 1 || logs[0].Type != "delete-project" ||
        logs[0].Project == nil || *(logs[0].Project) != project {
        t.Fatal("logs are not as expected from project deletion")
    }
}

func TestDeleteAsset(t *testing.T) {
    project := "foobar"
    asset := "stuff"

    t.Run("simple", func(t *testing.T) {
        reg, err := mockRegistryForDeletion(project, asset, []string{ "1", "2" })
        if err != nil {
            t.Fatalf("failed to mock up registry; %v", err) 
        }
        globals := newGlobalConfiguration(reg)

        reqpath, err := dumpRequest("delete_asset", fmt.Sprintf(`{ "project": "%s", "asset": "%s" }`, project, asset))
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }
        err = deleteAssetHandler(reqpath, &globals)
        if err == nil || !strings.Contains(err.Error(), "not authorized") {
            t.Fatal("unexpected authorization for non-admin")
        }

        self, err := identifyUser(reg)
        if err != nil {
            t.Fatalf("failed to identify self; %v", err)
        }
        globals.Administrators = append(globals.Administrators, self)
        err = deleteAssetHandler(reqpath, &globals)
        if err != nil {
            t.Fatalf("failed to delete an asset; %v", err)
        }

        project_dir := filepath.Join(reg, project)
        asset_dir := filepath.Join(project_dir, asset)
        if _, err := os.Stat(asset_dir); !errors.Is(err, os.ErrNotExist) {
            t.Fatal("failed to delete the asset directory")
        }

        usage, err := readUsage(project_dir)
        if err != nil {
            t.Fatalf("failed to read usage after deletion; %v", err)
        }
        if usage.Total != 0 {
            t.Fatal("expected zero usage after asset deletion")
        }

        // No-ops if repeated with already-deleted asset.
        err = deleteAssetHandler(reqpath, &globals)
        if err != nil {
            t.Fatalf("failed to delete a project; %v", err)
        }

        // Checking that inputs are valid.
        reqpath, err = dumpRequest("delete_asset", `{ "project": "foo" }`)
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        err = deleteAssetHandler(reqpath, &globals)
        if err == nil || !strings.Contains(err.Error(), "invalid 'asset'") {
            t.Fatal("fail to throw for invalid request")
        }

        // Checking that logs were correctly written.
        logs, err := readAllLogs(reg)
        if err != nil {
            t.Fatalf("failed to read all logs; %v", err)
        }

        if len(logs) != 1 || logs[0].Type != "delete-asset" ||
            logs[0].Project == nil || *(logs[0].Project) != project ||
            logs[0].Asset == nil || *(logs[0].Asset) != asset {
            t.Fatal("logs are not as expected from asset deletion")
        }
    })

    t.Run("forced", func(t *testing.T) {
        reg, err := mockRegistryForDeletion(project, asset, []string{ "1", "2" })
        if err != nil {
            t.Fatalf("failed to mock up registry; %v", err) 
        }

        // Nuke the manifest files.
        err = os.Remove(filepath.Join(reg, project, asset, "1", manifestFileName))
        if err != nil {
            t.Fatal(err)
        }

        globals := newGlobalConfiguration(reg)
        self, err := identifyUser(reg)
        if err != nil {
            t.Fatalf("failed to identify self; %v", err)
        }
        globals.Administrators = append(globals.Administrators, self)

        reqpath, err := dumpRequest("delete_asset", fmt.Sprintf(`{ "project": "%s", "asset": "%s", "force": false }`, project, asset))
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }
        err = deleteAssetHandler(reqpath, &globals)
        if err == nil || !strings.Contains(err.Error(), "manifest") {
            t.Errorf("expected the deletion to fail in the absence of a manifest; %v", err)
        }

        reqpath, err = dumpRequest("delete_asset", fmt.Sprintf(`{ "project": "%s", "asset": "%s", "force": true }`, project, asset))
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }
        err = deleteAssetHandler(reqpath, &globals)
        if err != nil {
            t.Fatal(err)
        }
        if _, err := os.Stat(filepath.Join(reg, project, asset)); err == nil || !errors.Is(err, os.ErrNotExist) {
            t.Error("expected the asset deletion to work correctly")
        }
    })
}

func TestDeleteVersion(t *testing.T) {
    project := "foobar"
    asset := "stuff"

    t.Run("basic", func(t *testing.T) {
        version := "random"
        reg, err := mockRegistryForDeletion(project, asset, []string{ version })
        if err != nil {
            t.Fatalf("failed to mock up registry; %v", err) 
        }

        reqpath, err := dumpRequest("delete_version", fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version))
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        globals := newGlobalConfiguration(reg)
        err = deleteVersionHandler(reqpath, &globals)
        if err == nil || !strings.Contains(err.Error(), "not authorized") {
            t.Fatal("unexpected authorization for non-admin")
        }

        self, err := identifyUser(reg)
        if err != nil {
            t.Fatalf("failed to identify self; %v", err)
        }
        globals.Administrators = append(globals.Administrators, self)
        err = deleteVersionHandler(reqpath, &globals)
        if err != nil {
            t.Fatalf("failed to delete a version; %v", err)
        }

        project_dir := filepath.Join(reg, project)
        asset_dir := filepath.Join(project_dir, asset)
        lost_version_dir := filepath.Join(asset_dir, version)
        if _, err := os.Stat(lost_version_dir); !errors.Is(err, os.ErrNotExist) {
            t.Fatal("failed to delete the version directory")
        }

        usage, err := readUsage(project_dir)
        if err != nil {
            t.Fatalf("failed to read usage after deletion; %v", err)
        }
        if usage.Total != 0 {
            t.Fatal("expected no usage after version deletion")
        }

        _, err = readLatest(asset_dir)
        if err == nil || !errors.Is(err, os.ErrNotExist) {
            t.Fatalf("latest version should not be present after deletion; %v", err)
        }

        // No-ops if repeated with already-deleted version.
        err = deleteVersionHandler(reqpath, &globals)
        if err != nil {
            t.Fatalf("failed to no-op for double-deleting a version; %v", err)
        }

        // Checking that inputs are valid.
        reqpath, err = dumpRequest("delete_version", `{ "project": "foo", "asset": "bar" }`)
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        err = deleteVersionHandler(reqpath, &globals)
        if err == nil || !strings.Contains(err.Error(), "invalid 'version'") {
            t.Fatal("fail to throw for invalid request")
        }

        // Checking that logs were correctly written.
        logs, err := readAllLogs(reg)
        if err != nil {
            t.Fatalf("failed to read all logs; %v", err)
        }

        if len(logs) != 1 || logs[0].Type != "delete-version" ||
            logs[0].Project == nil || *(logs[0].Project) != project ||
            logs[0].Asset == nil || *(logs[0].Asset) != asset ||
            logs[0].Version == nil || *(logs[0].Version) != version || 
            logs[0].Latest == nil || !*(logs[0].Latest) {
            t.Fatalf("logs are not as expected from version deletion; %v", logs)
        }
    })

    t.Run("multiple versions", func(t *testing.T) {
        for _, delete_oldest := range []bool{ true, false } {
            reg, err := mockRegistryForDeletion(project, asset, []string{ "boring_oldies", "hot_newness" })
            if err != nil {
                t.Fatalf("failed to mock up registry; %v", err) 
            }

            var to_delete string
            var survivor string
            if delete_oldest {
                to_delete = "boring_oldies"
                survivor = "hot_newness"
            } else {
                to_delete = "hot_newness"
                survivor = "boring_oldies"
            }

            reqpath, err := dumpRequest("delete_version", fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, to_delete))
            if err != nil {
                t.Fatalf("failed to dump a request type; %v", err)
            }

            globals := newGlobalConfiguration(reg)
            self, err := identifyUser(reg)
            if err != nil {
                t.Fatalf("failed to identify self; %v", err)
            }
            globals.Administrators = append(globals.Administrators, self)
            err = deleteVersionHandler(reqpath, &globals)
            if err != nil {
                t.Fatalf("failed to delete a version; %v", err)
            }

            project_dir := filepath.Join(reg, project)
            asset_dir := filepath.Join(project_dir, asset)
            lost_version_dir := filepath.Join(asset_dir, to_delete)
            if _, err := os.Stat(lost_version_dir); !errors.Is(err, os.ErrNotExist) {
                t.Fatal("failed to delete the version directory")
            }

            usage, err := readUsage(project_dir)
            if err != nil {
                t.Fatalf("failed to read usage after deletion; %v", err)
            }
            expected, err := computeVersionUsage(filepath.Join(asset_dir, survivor))
            if err != nil {
                t.Fatalf("failed to compute usage for the survivor; %v", err)
            }
            if usage.Total != expected {
                t.Fatal("mismatch in the expected usage after version deletion")
            }

            latest, err := readLatest(asset_dir)
            if err != nil {
                t.Fatalf("failed to read the latest version after deletion; %v", err)
            }
            if latest.Version != survivor {
                t.Fatal("mismatch in the expected latest version after version deletion")
            }

            // Checking that logs were correctly written.
            logs, err := readAllLogs(reg)
            if err != nil {
                t.Fatalf("failed to read all logs; %v", err)
            }

            if len(logs) != 1 || logs[0].Type != "delete-version" ||
                logs[0].Project == nil || *(logs[0].Project) != project ||
                logs[0].Asset == nil || *(logs[0].Asset) != asset ||
                logs[0].Version == nil || *(logs[0].Version) != to_delete || 
                logs[0].Latest == nil || *(logs[0].Latest) == delete_oldest {
                t.Fatalf("logs are not as expected from version deletion; %v", logs)
            }
        }
    })

    t.Run("probational", func(t *testing.T) {
        version := "ppp"
        reg, err := mockRegistryForDeletion(project, asset, []string{ version })
        if err != nil {
            t.Fatalf("failed to mock up registry; %v", err) 
        }

        // Turning it into a probational version.
        project_dir := filepath.Join(reg, project)
        asset_dir := filepath.Join(project_dir, asset)
        version_dir := filepath.Join(asset_dir, version)
        summ_path := filepath.Join(version_dir, summaryFileName)
        probational := true
        summary := summaryMetadata {
            UploadUserId: "urmom",
            UploadStart: "1999-09-19T19:09:19Z",
            UploadFinish: "1999-09-19T19:09:19Z",
            OnProbation: &probational,
        }
        err = dumpJson(summ_path, &summary)
        if err != nil {
            t.Fatal(err)
        }
        err = os.Remove(filepath.Join(asset_dir, latestFileName))
        if err != nil {
            t.Fatal(err)
        }

        reqpath, err := dumpRequest("delete_version", fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version))
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        globals := newGlobalConfiguration(reg)
        self, err := identifyUser(reg)
        if err != nil {
            t.Fatalf("failed to identify self; %v", err)
        }
        globals.Administrators = append(globals.Administrators, self)
        err = deleteVersionHandler(reqpath, &globals)
        if err != nil {
            t.Fatalf("failed to delete a version; %v", err)
        }

        if _, err := os.Stat(version_dir); !errors.Is(err, os.ErrNotExist) {
            t.Fatal("failed to delete the version directory")
        }

        usage, err := readUsage(project_dir)
        if err != nil {
            t.Fatalf("failed to read usage after deletion; %v", err)
        }
        if usage.Total != 0 {
            t.Fatal("expected no usage after version deletion")
        }

        lat, err := readLatest(asset_dir)
        if err == nil || !errors.Is(err, os.ErrNotExist) {
            fmt.Println(lat)
            t.Fatalf("latest version should not be present after deletion; %v", err)
        }

        // Checking that no logs were written.
        logs, err := readAllLogs(reg)
        if err != nil {
            t.Fatalf("failed to read all logs; %v", err)
        }
        if len(logs) != 0 {
            t.Fatalf("no logs should be generated after deleting a probational version; %v", logs[0])
        }
    })

    t.Run("forced", func(t *testing.T) {
        version1 := "no_manifest"
        version2 := "no_summary"
        reg, err := mockRegistryForDeletion(project, asset, []string{ version1, version2 })
        if err != nil {
            t.Fatalf("failed to mock up registry; %v", err) 
        }
        globals := newGlobalConfiguration(reg)

        self, err := identifyUser(reg)
        if err != nil {
            t.Fatalf("failed to identify self; %v", err)
        }
        globals.Administrators = append(globals.Administrators, self)

        err = os.Remove(filepath.Join(reg, project, asset, version1, manifestFileName))
        if err != nil {
            t.Fatal(err)
        }
        err = os.Remove(filepath.Join(reg, project, asset, version2, summaryFileName))
        if err != nil {
            t.Fatal(err)
        }

        // Checking we can force our way through without a manifest.
        reqpath, err := dumpRequest("delete_version", fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s", "force": false }`, project, asset, version1))
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }
        err = deleteVersionHandler(reqpath, &globals)
        if err == nil || !strings.Contains(err.Error(), "manifest") {
            t.Error("deletion should have failed without a manifest file")
        }

        reqpath, err = dumpRequest("delete_version", fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s", "force": true }`, project, asset, version1))
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }
        err = deleteVersionHandler(reqpath, &globals)
        if err != nil {
            t.Fatal(err)
        }
        if _, err := os.Stat(filepath.Join(reg, project, asset, version1)); err == nil || !errors.Is(err, os.ErrNotExist) {
            t.Error("expected the version deletion to work correctly")
        }

        // Checking we can force our way through without a summary.
        reqpath, err = dumpRequest("delete_version", fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s", "force": false }`, project, asset, version2))
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }
        err = deleteVersionHandler(reqpath, &globals)
        if err == nil || !strings.Contains(err.Error(), "summary") {
            t.Error("deletion should have failed without a summary file")
        }

        reqpath, err = dumpRequest("delete_version", fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s", "force": true }`, project, asset, version2))
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }
        err = deleteVersionHandler(reqpath, &globals)
        if err != nil {
            t.Fatal(err)
        }
        if _, err := os.Stat(filepath.Join(reg, project, asset, version2)); err == nil || !errors.Is(err, os.ErrNotExist) {
            t.Error("expected the version deletion to work correctly")
        }
    })
}

