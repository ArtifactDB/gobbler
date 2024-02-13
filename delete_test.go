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
    }

    err = os.WriteFile(filepath.Join(project_dir, usageFileName), []byte(fmt.Sprintf(`{ "total": %d }`, expected_size)), 0644)
    if err != nil {
        return "", fmt.Errorf("failed to write usage; %w", err)
    }

    err = os.WriteFile(filepath.Join(asset_dir, latestFileName), []byte(fmt.Sprintf(`{ "latest": "%s" }`, versions[len(versions) - 1])), 0644)
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

    err = deleteProjectHandler(reqpath, reg, nil)
    if err == nil || !strings.Contains(err.Error(), "not authorized") {
        t.Fatal("unexpected authorization for non-admin")
    }

    self, err := identifyUser(reg)
    if err != nil {
        t.Fatalf("failed to identify self; %v", err)
    }
    err = deleteProjectHandler(reqpath, reg, []string{ self })
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

    // Checking that inputs are valid.
    reqpath, err = dumpRequest("delete_project", "{}")
    if err != nil {
        t.Fatalf("failed to dump a request type; %v", err)
    }

    err = deleteProjectHandler(reqpath, reg, []string{ self })
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
    reg, err := mockRegistryForDeletion(project, asset, []string{ "1", "2" })
    if err != nil {
        t.Fatalf("failed to mock up registry; %v", err) 
    }

    reqpath, err := dumpRequest("delete_asset", fmt.Sprintf(`{ "project": "%s", "asset": "%s" }`, project, asset))
    if err != nil {
        t.Fatalf("failed to dump a request type; %v", err)
    }

    err = deleteAssetHandler(reqpath, reg, nil)
    if err == nil || !strings.Contains(err.Error(), "not authorized") {
        t.Fatal("unexpected authorization for non-admin")
    }

    self, err := identifyUser(reg)
    if err != nil {
        t.Fatalf("failed to identify self; %v", err)
    }
    err = deleteAssetHandler(reqpath, reg, []string{ self })
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

    // Checking that inputs are valid.
    reqpath, err = dumpRequest("delete_asset", `{ "project": "foo" }`)
    if err != nil {
        t.Fatalf("failed to dump a request type; %v", err)
    }

    err = deleteAssetHandler(reqpath, reg, []string{ self })
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
}

func TestDeleteVersion(t *testing.T) {
    project := "foobar"
    asset := "stuff"

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
            survivor = "hot_newness"
            to_delete = "boring_oldies"
        }

        reqpath, err := dumpRequest("delete_version", fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, to_delete))
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        err = deleteVersionHandler(reqpath, reg, nil)
        if err == nil || !strings.Contains(err.Error(), "not authorized") {
            t.Fatal("unexpected authorization for non-admin")
        }

        self, err := identifyUser(reg)
        if err != nil {
            t.Fatalf("failed to identify self; %v", err)
        }
        err = deleteVersionHandler(reqpath, reg, []string{ self })
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
        expected, err := computeUsage(filepath.Join(asset_dir, survivor), true)
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
        if latest.Latest != survivor {
            t.Fatal("mismatch in the expected latest version after version deletion")
        }

        // Checking that inputs are valid.
        reqpath, err = dumpRequest("delete_version", `{ "project": "foo", "asset": "bar" }`)
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        err = deleteVersionHandler(reqpath, reg, []string{ self })
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
            logs[0].Version == nil || *(logs[0].Version) != to_delete {
            t.Fatal("logs are not as expected from version deletion")
        }
    }
}

