package main

import (
    "testing"
    "os"
    "path/filepath"
    "fmt"
    "time"
    "strconv"
    "strings"
    "os/user"
    "errors"
)

func TestReadLatest(t *testing.T) {
    f, err := os.MkdirTemp("", "test-")
    if err != nil {
        t.Fatalf("failed to create tempdir; %v", err)
    }

    err = os.WriteFile(
        filepath.Join(f, LatestFileName),
        []byte(`{ "latest": "argle" }`),
        0644,
    )
    if err != nil {
        t.Fatalf("failed to create test ..latest; %v", err)
    }

    out, err := ReadLatest(f)
    if err != nil {
        t.Fatalf("failed to read test ..latest; %v", err)
    }

    if out.Latest != "argle" {
        t.Fatalf("unexpected 'latest' value")
    }
}

func TestrefreshLatestHandler(t *testing.T) {
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

    asset_name := "stuff"
    asset_dir := filepath.Join(project_dir, asset_name)
    err = os.Mkdir(asset_dir, 0755)
    if err != nil {
        t.Fatalf("failed to create asset directory; %v", err)
    }

    currently := time.Now()
    for i := 1; i <= 3; i++ {
        version_dir := filepath.Join(asset_dir, strconv.Itoa(i))
        err := os.Mkdir(version_dir, 0755)
        if err != nil {
            t.Fatalf("failed to create version directory; %v", err)
        }

        summ := SummaryMetadata {
            UploadUserId: "aaron",
            UploadStart: currently.Format(time.RFC3339),
            UploadFinish: currently.Add(time.Duration(i) * time.Minute).Format(time.RFC3339),
        }
        err = dump_json(filepath.Join(version_dir, SummaryFileName), &summ)
        if err != nil {
            t.Fatalf("failed to write the asset summary; %v", err)
        }
    }

    // Now formulating the request.
    reqpath, err := dump_request("refresh_latest", fmt.Sprintf(`{ "project": "%s", "asset": "%s" }`, project_name, asset_name))
    if err != nil {
        t.Fatalf("failed to write the request; %v", err)
    }

    err = refreshLatestHandler(reqpath, reg, nil)
    if err == nil || !strings.Contains(err.Error(), "not authorized") {
        t.Fatalf("unexpected authorization for refresh request")
    }

    self, err := user.Current()
    if err != nil {
        t.Fatalf("failed to find the current user; %v", err)
    }
    self_name := self.Username

    err = refreshLatestHandler(reqpath, reg, []string{ self_name })
    if err != nil {
        t.Fatalf("failed to perform the refresh; %v", err)
    }

    latest, err := ReadLatest(asset_dir)
    if err != nil {
        t.Fatalf("failed to read the latest version; %v", err)
    }
    if latest.Latest != "3" {
        t.Fatalf("latest version is not as expected")
    }

    // Injecting some probation.
    on_probation := true
    {
        version_dir := filepath.Join(asset_dir, "3")
        summ, err := ReadSummary(version_dir)
        if err != nil {
            t.Fatalf("failed to read version 3; %v", err)
        }

        summ.OnProbation = &on_probation
        err = dump_json(filepath.Join(version_dir, SummaryFileName), &summ)
        if err != nil {
            t.Fatalf("failed to update version summary; %v", err)
        }

        err = refreshLatestHandler(reqpath, reg, []string{ self_name })
        if err != nil {
            t.Fatalf("failed to perform the refresh; %v", err)
        }

        latest, err := ReadLatest(asset_dir)
        if err != nil {
            t.Fatalf("failed to read the latest version; %v", err)
        }
        if latest.Latest != "2" {
            t.Fatalf("latest version is not as expected after probation")
        }
    }

    // Making them all probational.
    {
        for i := 1; i <= 2; i++ {
            version_dir := filepath.Join(asset_dir, strconv.Itoa(i))
            summ, err := ReadSummary(version_dir)
            if err != nil {
                t.Fatalf("failed to read version %d; %v", i, err)
            }

            summ.OnProbation = &on_probation
            err = dump_json(filepath.Join(version_dir, SummaryFileName), &summ)
            if err != nil {
                t.Fatalf("failed to update version summary; %v", err)
            }
        }

        err = refreshLatestHandler(reqpath, reg, []string{ self_name })
        if err != nil {
            t.Fatalf("failed to perform the refresh; %v", err)
        }
        _, err := ReadLatest(asset_dir)
        if err == nil || !errors.Is(err, os.ErrNotExist) {
            t.Fatalf("latest version should not exist for all-probational asset; %v", err)
        }
    }

}
