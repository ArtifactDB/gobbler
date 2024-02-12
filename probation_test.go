package main

import (
    "testing"
    "os"
    "path/filepath"
    "fmt"
    "strings"
    "errors"
)

func mockProbationVersion(reg, project, asset, version string) error {
    project_dir := filepath.Join(reg, project)
    err := os.Mkdir(project_dir, 0755)
    if err != nil {
        return fmt.Errorf("failed to create a project directory; %w", err)
    }

    err = os.WriteFile(
        filepath.Join(project_dir, permissionsFileName),
        []byte(`{ "owners": [], "uploaders": [] }`),
        0644,
    )
    if err != nil {
        return fmt.Errorf("failed to create some mock permissions; %w", err)
    }

    asset_dir := filepath.Join(project_dir, asset)
    err = os.Mkdir(asset_dir, 0755)
    if err != nil {
        return fmt.Errorf("failed to create an asset directory; %w", err)
    }

    version_dir := filepath.Join(asset_dir, version)
    err = os.Mkdir(version_dir, 0755)
    if err != nil {
        return fmt.Errorf("failed to create a version directory; %w", err)
    }

    err = os.WriteFile(
        filepath.Join(version_dir, summaryFileName),
        []byte(`
{
    "upload_user_id": "cynthia",
    "upload_start": "2020-02-02T02:02:02Z",
    "upload_finish": "2020-02-02T02:02:20Z",
    "on_probation": true
}`),
        0644,
    )
    if err != nil {
        return fmt.Errorf("failed to create a mock summary; %w", err)
    }

    contents := "ARGLEFARGLE"
    err = os.WriteFile(filepath.Join(version_dir, "random"), []byte(contents), 0644)
    if err != nil {
        return fmt.Errorf("failed to create some mock files; %w", err)
    }

    err = os.WriteFile(filepath.Join(project_dir, usageFileName), []byte(fmt.Sprintf(`{ "total": %d }`, len(contents))), 0644)
    if err != nil {
        return fmt.Errorf("failed to create mock usage; %w", err)
    }

    return nil
}

func TestApproveProbationHandler(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    project := "dawn"
    asset := "sinnoh"
    version := "foo"
    err = mockProbationVersion(reg, project, asset, version)
    if err != nil {
        t.Fatalf("failed to create a mock version; %v", err)
    }

    reqpath, err := dumpRequest("approve_probation", fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version))
    if err != nil {
        t.Fatalf("failed to dump a request type; %v", err)
    }

    // Lack of authorization fails.
    err = approveProbationHandler(reqpath, reg, nil)
    if err == nil || !strings.Contains(err.Error(), "not authorized") {
        t.Fatalf("failed to approve probation; %v", err)
    }

    self, err := identifyUser(reg)
    if err != nil {
        t.Fatalf("failed to identify self; %v", err)
    }
    err = approveProbationHandler(reqpath, reg, []string{ self })
    if err != nil {
        t.Fatalf("failed to approve probation; %v", err)
    }

    summ, err := readSummary(filepath.Join(reg, project, asset, version))
    if err != nil {
        t.Fatalf("failed to read the summary; %v", err)
    }
    if summ.OnProbation != nil {
        t.Fatal("version should not be on probation after approval")
    }

    latest, err := readLatest(filepath.Join(reg, project, asset))
    if err != nil {
        t.Fatalf("failed to read the latest; %v", err)
    }
    if latest.Latest != version {
        t.Fatal("latest version should be updated after approval")
    }

    // Repeated approval attempts fail.
    err = approveProbationHandler(reqpath, reg, []string{ self })
    if err == nil || !strings.Contains(err.Error(), "not on probation") {
        t.Fatal("expected failure for non-probational version")
    }
}

func TestApproveProbationHandlerNotLatest(t *testing.T) {
    for _, other_latest := range []bool{ true, false } {
        reg, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatalf("failed to create the registry; %v", err)
        }

        project := "dawn"
        asset := "sinnoh"
        version := "foo"
        err = mockProbationVersion(reg, project, asset, version)
        if err != nil {
            t.Fatalf("failed to create a mock version; %v", err)
        }

        // Mocking up another version that was either earlier or later.
        version2 := "bar"
        project_dir := filepath.Join(reg, project)
        asset_dir := filepath.Join(project_dir, asset)
        version_dir := filepath.Join(asset_dir, version2)
        err = os.MkdirAll(version_dir, 0755)
        if err != nil {
            t.Fatalf("failed to create a new version directory; %v", err)
        }

        var new_time string
        if other_latest {
            new_time = "2999-02-02T02:02:02Z"
        } else {
            new_time = "1999-02-02T02:02:02Z"
        }
        err = os.WriteFile(
            filepath.Join(version_dir, summaryFileName),
            []byte(fmt.Sprintf(`
{
    "upload_user_id": "cynthia",
    "upload_start": "2020-02-02T02:02:02Z",
    "upload_finish": "%s",
    "on_probation": true
}`, new_time)),
            0644,
        )
        if err != nil {
            t.Fatalf("failed to create a mock summary; %v", err)
        }

        err = os.WriteFile(filepath.Join(asset_dir, latestFileName), []byte(fmt.Sprintf(`{ "latest": "%s" }`, version2)), 0644)
        if err != nil {
            t.Fatalf("failed to write the latest file; %v", err)
        }

        // Running the approval.
        reqpath, err := dumpRequest("approve_probation", fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version))
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        self, err := identifyUser(reg)
        if err != nil {
            t.Fatalf("failed to identify self; %v", err)
        }
        err = approveProbationHandler(reqpath, reg, []string{ self })
        if err != nil {
            t.Fatalf("failed to approve probation; %v", err)
        }

        // Checking that we are (or not) the latest version.
        latest, err := readLatest(filepath.Join(reg, project, asset))
        if err != nil {
            t.Fatalf("failed to read the latest; %v", err)
        }

        if other_latest {
            if latest.Latest != version2 {
                t.Fatal("latest version should not be updated after approval")
            }
        } else {
            if latest.Latest != version {
                t.Fatal("latest version should be updated after approval")
            }
        }
    }
}

func TestRejectProbationHandler(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    project := "dawn"
    asset := "sinnoh"
    version := "foo"
    err = mockProbationVersion(reg, project, asset, version)
    if err != nil {
        t.Fatalf("failed to create a mock version; %v", err)
    }

    reqpath, err := dumpRequest("reject_probation", fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version))
    if err != nil {
        t.Fatalf("failed to dump a request type; %v", err)
    }

    self, err := identifyUser(reg)
    if err != nil {
        t.Fatalf("failed to identify self; %v", err)
    }
    err = rejectProbationHandler(reqpath, reg, []string{ self })
    if err != nil {
        t.Fatalf("failed to reject probation; %v", err)
    }

    project_dir := filepath.Join(reg, project)
    if _, err := os.Stat(filepath.Join(project_dir, asset, version)); err == nil || !errors.Is(err, os.ErrNotExist) {
        t.Fatalf("failed to delete the probational directory; %v", err)
    }

    usage, err := readUsage(project_dir)
    if err != nil {
        t.Fatalf("failed to read the project usage; %v", err)
    }
    if usage.Total != 0 {
        t.Fatalf("expected the project usage to be zero, not %d", usage.Total)
    }
}
