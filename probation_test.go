package main

import (
    "testing"
    "os"
    "path/filepath"
    "fmt"
    "strings"
    "errors"
    "time"
    "sort"
    "context"
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

    err = reindexDirectory(reg, project, asset, version, []string{}, context.Background())
    if err != nil {
        return fmt.Errorf("failed to reindex the directory; %w", err)
    }

    err = os.WriteFile(filepath.Join(project_dir, usageFileName), []byte(fmt.Sprintf(`{ "total": %d }`, len(contents))), 0644)
    if err != nil {
        return fmt.Errorf("failed to create mock usage; %w", err)
    }

    return nil
}

func TestApproveProbationHandler(t *testing.T) {
    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()

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
    globals := newGlobalConfiguration(reg)
    err = approveProbationHandler(reqpath, &globals, ctx)
    if err == nil || !strings.Contains(err.Error(), "not authorized") {
        t.Fatalf("failed to approve probation; %v", err)
    }

    self, err := identifyUser(reg)
    if err != nil {
        t.Fatalf("failed to identify self; %v", err)
    }
    globals.Administrators = append(globals.Administrators, self)
    err = approveProbationHandler(reqpath, &globals, ctx)
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
    if latest.Version != version {
        t.Fatal("latest version should be updated after approval")
    }

    // Checking that logs are created.
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

    // Repeated approval attempts fail.
    err = approveProbationHandler(reqpath, &globals, ctx)
    if err == nil || !strings.Contains(err.Error(), "not on probation") {
        t.Fatal("expected failure for non-probational version")
    }
}

func TestApproveProbationHandlerNotLatest(t *testing.T) {
    ctx := context.Background()

    for _, other_latest := range []bool{ true, false } {
        reg, err := constructMockRegistry()
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

        err = os.WriteFile(filepath.Join(asset_dir, latestFileName), []byte(fmt.Sprintf(`{ "version": "%s" }`, version2)), 0644)
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
        globals := newGlobalConfiguration(reg)
        globals.Administrators = append(globals.Administrators, self)
        err = approveProbationHandler(reqpath, &globals, ctx)
        if err != nil {
            t.Fatalf("failed to approve probation; %v", err)
        }

        // Checking that we are (or not) the latest version.
        latest, err := readLatest(filepath.Join(reg, project, asset))
        if err != nil {
            t.Fatalf("failed to read the latest; %v", err)
        }

        if other_latest {
            if latest.Version != version2 {
                t.Fatal("latest version should not be updated after approval")
            }
        } else {
            if latest.Version != version {
                t.Fatal("latest version should be updated after approval")
            }
        }

        // Checking that a suitable log is created.
        logs, err := readAllLogs(reg)
        if err != nil {
            t.Fatalf("failed to read the logs; %v", err)
        }
        if len(logs) != 1 {
            t.Fatalf("expected exactly one entry in the log directory")
        }
        if logs[0].Latest == nil || *(logs[0].Latest) == other_latest {
            t.Fatalf("unexpected latest flag after probation approval for first log in %q", reg)
        }
    }
}

func TestRejectProbationHandler(t *testing.T) {
    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg)

    self, err := identifyUser(reg)
    if err != nil {
        t.Fatalf("failed to identify self; %v", err)
    }
    globals.Administrators = append(globals.Administrators, self)

    ctx := context.Background()

    t.Run("simple", func(t *testing.T) {
        project := "dawn"
        asset := "sinnoh"
        version := "foo"
        err := mockProbationVersion(reg, project, asset, version)
        if err != nil {
            t.Fatalf("failed to create a mock version; %v", err)
        }

        reqpath, err := dumpRequest("reject_probation", fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s" }`, project, asset, version))
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }
        err = rejectProbationHandler(reqpath, &globals, ctx)
        if err != nil {
            t.Fatalf("failed to reject probation; %v", err)
        }

        project_dir := filepath.Join(reg, project)
        if _, err := os.Stat(filepath.Join(project_dir, asset, version)); err == nil || !errors.Is(err, os.ErrNotExist) {
            t.Fatal("failed to delete the probational directory")
        }

        usage, err := readUsage(project_dir)
        if err != nil {
            t.Fatalf("failed to read the project usage; %v", err)
        }
        if usage.Total != 0 {
            t.Fatalf("expected the project usage to be zero, not %d", usage.Total)
        }
    })

    t.Run("forced", func(t *testing.T) {
        project := "serena"
        asset := "kalos"
        version := "bar"
        err := mockProbationVersion(reg, project, asset, version)
        if err != nil {
            t.Fatalf("failed to create a mock version; %v", err)
        }

        err = os.Remove(filepath.Join(reg, project, asset, version, manifestFileName))
        if err != nil {
            t.Fatal(err)
        }

        reqpath, err := dumpRequest("reject_probation", fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s", "force": false }`, project, asset, version))
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }
        err = rejectProbationHandler(reqpath, &globals, ctx)
        if err == nil || !strings.Contains(err.Error(), "manifest") {
            t.Error("expected request to fail when manifest is removed")
        }

        reqpath, err = dumpRequest("reject_probation", fmt.Sprintf(`{ "project": "%s", "asset": "%s", "version": "%s", "force": true }`, project, asset, version))
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }
        err = rejectProbationHandler(reqpath, &globals, ctx)
        if err != nil {
            t.Error(err)
        }
        if _, err := os.Stat(filepath.Join(project, asset, version)); err == nil || !errors.Is(err, os.ErrNotExist) {
            t.Error("expected probational version directory to be deleted")
        }
    })
}

func TestPurgeOldProbationalVersions(t *testing.T) {
    project := "dawn"
    asset := "sinnoh"

    ctx := context.Background()

    mockProbationalRegistry := func(reg string) error {
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

        // Mocking up a few probational versions.
        for _, version := range []string{ "foo", "bar", "stuff", "whee" } {
            version_dir := filepath.Join(asset_dir, version)
            err = os.Mkdir(version_dir, 0755)
            if err != nil {
                return fmt.Errorf("failed to create a version directory; %w", err)
            }

            var summary string
            if version == "foo" {
                summary = `{
    "upload_user_id": "cynthia",
    "upload_start": "2020-02-02T02:02:02Z",
    "upload_finish": "2020-02-02T02:02:20Z",
    "on_probation": false
}`
            } else if version == "bar" {
                upload_time := time.Now().Add(-10 * time.Minute).Format(time.RFC3339)
                summary = fmt.Sprintf(`{
    "upload_user_id": "cynthia",
    "upload_start": "%s",
    "upload_finish": "%s",
    "on_probation": true
}`, upload_time, upload_time)
            } else if version == "stuff" {
                summary = `{
    "upload_user_id": "cynthia",
    "upload_start": "2020-02-02T02:02:02Z",
    "upload_finish": "2020-02-02T02:02:20Z"
}`
            } else {
                upload_time := time.Now().Add(-10 * time.Hour).Format(time.RFC3339)
                summary = fmt.Sprintf(`{
    "upload_user_id": "cynthia",
    "upload_start": "%s",
    "upload_finish": "%s",
    "on_probation": true
}`, upload_time, upload_time)
            }

            err = os.WriteFile(filepath.Join(version_dir, summaryFileName), []byte(summary), 0644)
            if err != nil {
                return fmt.Errorf("failed to create a mock summary; %w", err)
            }

            contents := "ARGLEFARGLE"
            err = os.WriteFile(filepath.Join(version_dir, "random"), []byte(contents), 0644)
            if err != nil {
                return fmt.Errorf("failed to create some mock files; %w", err)
            }

            err = reindexDirectory(reg, project, asset, version, []string{}, ctx)
            if err != nil {
                return fmt.Errorf("failed to reindex the directory; %w", err)
            }

            err = os.WriteFile(filepath.Join(project_dir, usageFileName), []byte(fmt.Sprintf(`{ "total": %d }`, len(contents))), 0644)
            if err != nil {
                return fmt.Errorf("failed to create mock usage; %w", err)
            }
        }

        return nil
    }

    t.Run("zero hours", func(t *testing.T) {
        reg, err := constructMockRegistry()
        if err != nil {
            t.Fatal(err)
        }

        err = mockProbationalRegistry(reg)
        if err != nil {
            t.Fatal(err)
        }

        globals := newGlobalConfiguration(reg)
        errs := purgeOldProbationalVersions(&globals, 0)
        if len(errs) != 0 {
            t.Fatal(errs[0])
        }

        available_versions, err := listUserDirectories(filepath.Join(globals.Registry, project, asset))
        sort.Strings(available_versions)
        if len(available_versions) != 2 || available_versions[0] != "foo" || available_versions[1] != "stuff" {
            t.Errorf("unexpected versions remaining after probational purge; %v", available_versions)
        }
    })

    t.Run("one hour", func(t *testing.T) {
        reg, err := constructMockRegistry()
        if err != nil {
            t.Fatal(err)
        }

        err = mockProbationalRegistry(reg)
        if err != nil {
            t.Fatal(err)
        }

        globals := newGlobalConfiguration(reg)
        errs := purgeOldProbationalVersions(&globals, time.Hour)
        if len(errs) != 0 {
            t.Fatal(errs[0])
        }

        available_versions, err := listUserDirectories(filepath.Join(globals.Registry, project, asset))
        sort.Strings(available_versions)
        if len(available_versions) != 3 || available_versions[0] != "bar" || available_versions[1] != "foo" || available_versions[2] != "stuff" {
            t.Errorf("unexpected versions remaining after probational purge; %v", available_versions)
        }
    })

    t.Run("one day", func(t *testing.T) {
        reg, err := constructMockRegistry()
        if err != nil {
            t.Fatal(err)
        }

        err = mockProbationalRegistry(reg)
        if err != nil {
            t.Fatal(err)
        }

        globals := newGlobalConfiguration(reg)
        errs := purgeOldProbationalVersions(&globals, time.Hour * 24)
        if len(errs) != 0 {
            t.Fatal(errs[0])
        }

        available_versions, err := listUserDirectories(filepath.Join(globals.Registry, project, asset))
        sort.Strings(available_versions)
        if len(available_versions) != 4 {
            t.Errorf("unexpected versions remaining after probational purge; %v", available_versions)
        }
    })

}
