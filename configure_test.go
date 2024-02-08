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

func TestIsBadName(t *testing.T) {
    var err error

    err = is_bad_name("..foo")
    if err == nil || !strings.Contains(err.Error(), "..")  {
        t.Fatal("failed to stop on '..'") 
    }

    err = is_bad_name("")
    if err == nil || !strings.Contains(err.Error(), "empty") {
        t.Fatal("failed to stop on an empty name")
    }

    err = is_bad_name("asda/a")
    if err == nil || !strings.Contains(err.Error(), "/") {
        t.Fatal("failed to stop in the presence of a forward slash")
    }

    err = is_bad_name("asda\\asdasd")
    if err == nil || !strings.Contains(err.Error(), "\\") {
        t.Fatal("failed to stop in the presence of a backslash")
    }
}

func TestIncrementSeries(t *testing.T) {
    for _, prefix := range []string{ "V", "" } {
        dir, err := ioutil.TempDir("", "")
        if err != nil {
            t.Fatalf("failed to create the temporary directory; %v", err)
        }

        candidate, err := increment_series(prefix, dir)
        if err != nil {
            t.Fatalf("failed to initialize the series; %v", err)
        }
        if candidate != prefix + "1" {
            t.Fatalf("initial value of the series should be 1, got %s", candidate)
        }

        candidate, err = increment_series(prefix, dir)
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
        candidate, err = increment_series(prefix, dir)
        if err != nil {
            t.Fatalf("failed to update the series after conflict; %v", err)
        }
        if candidate != prefix + "4" {
            t.Fatal("next value of the series should be 4")
        }

        // Injecting a different value.
        series_path := increment_series_path(prefix, dir)
        err = os.WriteFile(series_path, []byte("100"), 0644)
        if err != nil {
            t.Fatalf("failed to overwrite the series file")
        }
        candidate, err = increment_series(prefix, dir)
        if err != nil {
            t.Fatalf("failed to update the series after overwrite; %v", err)
        }
        if candidate != prefix + "101" {
            t.Fatal("next value of the series should be 101")
        }
    }
}

func setup_for_configure_test(request string) (string, string, error) {
    reg, err := ioutil.TempDir("", "")
    if err != nil {
        return "", "", fmt.Errorf("failed to create the registry; %w", err)
    }

    dir, err := ioutil.TempDir("", "")
    if err != nil {
        return "", "", fmt.Errorf("failed to create the temporary directory; %w", err)
    }

    err = os.WriteFile(filepath.Join(dir, "_DETAILS"), []byte(request), 0644)
    if err != nil {
        return "", "", fmt.Errorf("failed to write transfer request details; %w", err)
    }

    return reg, dir, nil
}

func TestConfigureNewProjectBasic(t *testing.T) {
    registry, src, err := setup_for_configure_test("{ \"project\": \"foo\", \"asset\": \"BAR\", \"version\": \"whee\" }")
    if err != nil {
        t.Fatal(err)
    }

    config, err := Configure(src, registry)
    if err != nil {
        t.Fatalf("failed complete configuration; %v", err)
    }

    if config.Project != "foo" {
        t.Fatalf("unexpected value for the project name (%s)", config.Project)
    }

    if config.Asset != "BAR" {
        t.Fatalf("unexpected value for the asset name (%s)", config.Asset)
    }

    if config.Version != "whee" {
        t.Fatalf("unexpected value for the version name (%s)", config.Version)
    }

    // Checking the various bits and pieces.
    {
        perm_raw, err := os.ReadFile(filepath.Join(registry, config.Project, "..permissions"))
        if err != nil {
            t.Fatalf("failed to read the permissions; %v", err)
        }
        deets := struct { Owners []string `json:"owners"` }{}
        err = json.Unmarshal(perm_raw, &deets)
        if err != nil {
            t.Fatalf("failed to parse the permissions; %v", err)
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

func TestConfigureNewProjectBasicFailures(t *testing.T) {
    {
        registry, src, err := setup_for_configure_test("{ \"project\": \"FOO\", \"asset\": \"BAR\", \"version\": \"whee\" }")
        if err != nil {
            t.Fatal(err)
        }
        _, err = Configure(src, registry)
        if err == nil || !strings.Contains(err.Error(), "uppercase") {
            t.Fatal("configuration should fail for upper-cased project names")
        }
    }

    {
        registry, src, err := setup_for_configure_test("{ \"project\": \"..foo\", \"asset\": \"BAR\", \"version\": \"whee\" }")
        if err != nil {
            t.Fatal(err)
        }
        _, err = Configure(src, registry)
        if err == nil || !strings.Contains(err.Error(), "invalid project name") {
            t.Fatal("configuration should fail for invalid project name")
        }
    }

    {
        registry, src, err := setup_for_configure_test("{ \"project\": \"foo\", \"asset\": \"..BAR\", \"version\": \"whee\" }")
        if err != nil {
            t.Fatal(err)
        }
        _, err = Configure(src, registry)
        if err == nil || !strings.Contains(err.Error(), "invalid asset name") {
            t.Fatal("configuration should fail for invalid asset name")
        }
    }

    {
        registry, src, err := setup_for_configure_test("{ \"project\": \"foo\", \"asset\": \"BAR\", \"version\": \"..whee\" }")
        if err != nil {
            t.Fatal(err)
        }
        _, err = Configure(src, registry)
        if err == nil || !strings.Contains(err.Error(), "invalid version name") {
            t.Fatal("configuration should fail for invalid version name")
        }
    }

    {
        registry, src, err := setup_for_configure_test("{ \"project\": \"foo\", \"asset\": \"BAR\" }")
        if err != nil {
            t.Fatal(err)
        }
        _, err = Configure(src, registry)
        if err == nil || !strings.Contains(err.Error(), "without a version series") {
            t.Fatal("configuration should fail for missing versions")
        }
    }
}

func TestConfigureNewProjectSeries(t *testing.T) {
    registry, src, err := setup_for_configure_test("{ \"prefix\": \"FOO\", \"asset\": \"BAR\", \"version\": \"whee\" }")
    if err != nil {
        t.Fatal(err)
    }

    config, err := Configure(src, registry)
    if err != nil {
        t.Fatalf("failed complete configuration; %v", err)
    }
    if config.Project != "FOO1" {
        t.Fatalf("unexpected value for the project name (%s)", config.Project)
    }

    // Check check that everything was created.
    if _, err := os.Stat(filepath.Join(registry, config.Project, "..permissions")); err != nil {
        t.Fatalf("permissions file was not created")
    }

    config, err = Configure(src, registry)
    if err != nil {
        t.Fatalf("failed complete configuration; %v", err)
    }
    if config.Project != "FOO2" {
        t.Fatalf("unexpected value for the project name (%s)", config.Project)
    }
}

func TestConfigureNewProjectSeriesFailures(t *testing.T) {
    registry, src, err := setup_for_configure_test("{ \"asset\": \"BAR\", \"version\": \"whee\" }")
    if err != nil {
        t.Fatal(err)
    }

    _, err = Configure(src, registry)
    if err == nil || !strings.Contains(err.Error(), "expected a 'prefix'") {
        t.Fatalf("configuration should have failed without a prefix")
    }

    err = os.WriteFile(filepath.Join(src, "_DETAILS"), []byte("{ \"prefix\": \"foo\", \"asset\": \"BAR\", \"version\": \"whee\" }"), 0644)
    if err != nil {
        t.Fatalf("failed to write a new request; %v", err)
    }

    _, err = Configure(src, registry)
    if err == nil || !strings.Contains(err.Error(), "only uppercase") {
        t.Fatalf("configuration should have failed with non-uppercase prefix")
    }
}
