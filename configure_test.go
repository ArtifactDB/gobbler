package main

import (
    "testing"
    "io/ioutil"
    "path/filepath"
    "os"
    "strings"
    "fmt"
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

func TestConfigureBasic(t *testing.T) {
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
}
