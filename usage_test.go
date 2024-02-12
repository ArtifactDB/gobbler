package main

import (
    "testing"
    "os"
    "path/filepath"
    "strings"
    "fmt"
    "os/user"
)

func TestReadUsage(t *testing.T) {
    f, err := os.MkdirTemp("", "test-")
    if err != nil {
        t.Fatalf("failed to create tempdir; %v", err)
    }

    err = os.WriteFile(
        filepath.Join(f, usageFileName),
        []byte(`{ "total": 9999 }`),
        0644,
    )
    if err != nil {
        t.Fatalf("failed to create test ..usage; %v", err)
    }

    out, err := readUsage(f)
    if err != nil {
        t.Fatalf("failed to read test ..usage; %v", err)
    }

    if out.Total != 9999 {
        t.Fatalf("unexpected 'total' value")
    }
}

func TestComputeUsage(t *testing.T) {
    // Mocking up a directory.
    src, err := os.MkdirTemp("", "test-")
    if err != nil {
        t.Fatalf("failed to create tempdir; %v", err)
    }

    expected_size := 0

    msg := "grass,poison"
    err = os.WriteFile(filepath.Join(src, "type"), []byte(msg), 0644)
    expected_size += len(msg)
    if err != nil {
        t.Fatalf("failed to create mock file; %v", err)
    }

    err = os.Mkdir(filepath.Join(src, "evolution"), 0755)
    if err != nil {
        t.Fatalf("failed to create mock directory; %v", err)
    }
    msg = "ivysaur"
    err = os.WriteFile(filepath.Join(src, "evolution", "last"), []byte(msg), 0644)
    expected_size += len(msg)
    if err != nil {
        t.Fatalf("failed to create mock file; %v", err)
    }

    err = os.Mkdir(filepath.Join(src, "moves"), 0755)
    if err != nil {
        t.Fatalf("failed to create mock directory; %v", err)
    }
    err = os.Mkdir(filepath.Join(src, "moves", "grass"), 0755)
    if err != nil {
        t.Fatalf("failed to create mock directory; %v", err)
    }
    msg = "120"
    expected_size += len(msg)
    err = os.WriteFile(filepath.Join(src, "moves", "grass", "solar_beam"), []byte(msg), 0644)
    if err != nil {
        t.Fatalf("failed to create mock file; %v", err)
    }
    msg = "55"
    expected_size += len(msg)
    err = os.WriteFile(filepath.Join(src, "moves", "grass", "razor_leaf"), []byte(msg), 0644)
    if err != nil {
        t.Fatalf("failed to create mock file; %v", err)
    }

    // Actually running some tests.
    total, err := computeUsage(src, true)
    if err != nil {
        t.Fatalf("failed to create compute usage; %v", err)
    }
    if total != int64(expected_size) {
        t.Fatalf("sum of file sizes is different from expected (%d, got %d)", expected_size, total)
    }

    err = os.Symlink(
        filepath.Join(src, "moves", "grass", "razor_leaf"), 
        filepath.Join(src, "moves", "grass", "vine_whip"),
    )
    if err != nil {
        t.Fatalf("failed to create mock file; %v", err)
    }

    total, err = computeUsage(src, true)
    if err != nil {
        t.Fatalf("failed to create compute usage; %v", err)
    }
    if total != int64(expected_size) {
        t.Fatalf("sum of file sizes is different from expected (%d, got %d) when ignoring soft links", expected_size, total)
    }

    total, err = computeUsage(src, false)
    if err != nil {
        t.Fatalf("failed to create compute usage; %v", err)
    }
    if total != int64(expected_size + len(msg)) {
        t.Fatalf("sum of file sizes is different from expected (%d, got %d) when including soft links", expected_size, total)
    }

    // Prohibit links to directories.
    err = os.Symlink(
        filepath.Join(src, "moves", "grass"),
        filepath.Join(src, "moves", "poison"),
    )
    if err != nil {
        t.Fatalf("failed to create mock file; %v", err)
    }

    _, err = computeUsage(src, false)
    if err == nil || !strings.Contains(err.Error(), "symlinks to directories") {
        t.Fatalf("expected a failure in the presence of symlink to a directory")
    }
}

func TestRefreshUsageHandler(t *testing.T) {
    // Mocking up something interesting.
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    project_name := "foobar"
    project_dir := filepath.Join(reg, project_name)
    err = os.Mkdir(project_dir, 0755)
    if err != nil {
        t.Fatalf("failed to create project directory; %v", err)
    }

    expected_size := int64(0)
    for _, asset := range []string{ "WHEE", "STUFF", "BLAH" } {
        version_dir := filepath.Join(project_dir, asset)
        err := os.Mkdir(version_dir, 0755)
        if err != nil {
            t.Fatalf("failed to create asset directory; %v", err)
        }

        message := "I am " + asset
        err = os.WriteFile(filepath.Join(version_dir, "thingy"), []byte(message), 0644)
        if err != nil {
            t.Fatalf("failed to write a mock file; %v", err)
        }

        expected_size += int64(len(message))
    }

    // Running the latest information.
    reqpath, err := dumpRequest("refresh_usage", fmt.Sprintf(`{ "project": "%s" }`, project_name))
    if err != nil {
        t.Fatalf("failed to write the request; %v", err)
    }

    err = refreshUsageHandler(reqpath, reg, nil)
    if err == nil || !strings.Contains(err.Error(), "not authorized") {
        t.Fatalf("unexpected authorization for refresh request")
    }

    self, err := user.Current()
    if err != nil {
        t.Fatalf("failed to find the current user; %v", err)
    }
    self_name := self.Username

    err = refreshUsageHandler(reqpath, reg, []string{ self_name })
    if err != nil {
        t.Fatalf("failed to perform the refresh; %v", err)
    }

    used, err := readUsage(project_dir)
    if err != nil {
        t.Fatalf("failed to read the usage request; %v", err)
    }
    if used.Total != expected_size {
        t.Fatalf("usage is not as expected")
    }
}
