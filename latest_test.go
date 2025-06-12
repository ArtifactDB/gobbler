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
    "context"
)

func TestReadLatest(t *testing.T) {
    f, err := os.MkdirTemp("", "test-")
    if err != nil {
        t.Fatalf("failed to create tempdir; %v", err)
    }

    err = os.WriteFile(
        filepath.Join(f, latestFileName),
        []byte(`{ "version": "argle" }`),
        0644,
    )
    if err != nil {
        t.Fatalf("failed to create test ..latest; %v", err)
    }

    out, err := readLatest(f)
    if err != nil {
        t.Fatalf("failed to read test ..latest; %v", err)
    }

    if out.Version != "argle" {
        t.Fatalf("unexpected 'latest' value")
    }
}

func TestRefreshLatestHandler(t *testing.T) {
    // Mocking up something interesting.
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    ctx := context.Background()

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

        summ := summaryMetadata {
            UploadUserId: "aaron",
            UploadStart: currently.Format(time.RFC3339),
            UploadFinish: currently.Add(time.Duration(i) * time.Minute).Format(time.RFC3339),
        }
        err = dumpJson(filepath.Join(version_dir, summaryFileName), &summ)
        if err != nil {
            t.Fatalf("failed to write the asset summary; %v", err)
        }
    }

    // Now formulating the request.
    reqpath, err := dumpRequest("refresh_latest", fmt.Sprintf(`{ "project": "%s", "asset": "%s" }`, project_name, asset_name))
    if err != nil {
        t.Fatalf("failed to write the request; %v", err)
    }

    globals := newGlobalConfiguration(reg)
    _, err = refreshLatestHandler(reqpath, &globals, ctx)
    if err == nil || !strings.Contains(err.Error(), "not authorized") {
        t.Fatalf("unexpected authorization for refresh request")
    }

    self, err := user.Current()
    if err != nil {
        t.Fatalf("failed to find the current user; %v", err)
    }
    self_name := self.Username
    globals.Administrators = append(globals.Administrators, self_name)
    res, err := refreshLatestHandler(reqpath, &globals, ctx)
    if err != nil {
        t.Fatalf("failed to perform the refresh; %v", err)
    }
    if res.Version != "3" {
        t.Fatal("latest version is not as expected")
    }

    latest, err := readLatest(asset_dir)
    if err != nil {
        t.Fatalf("failed to read the latest version; %v", err)
    }
    if latest.Version != "3" {
        t.Fatal("latest version is not as expected")
    }

    // Injecting some probation.
    on_probation := true
    {
        version_dir := filepath.Join(asset_dir, "3")
        summ, err := readSummary(version_dir)
        if err != nil {
            t.Fatalf("failed to read version 3; %v", err)
        }

        summ.OnProbation = &on_probation
        err = dumpJson(filepath.Join(version_dir, summaryFileName), &summ)
        if err != nil {
            t.Fatalf("failed to update version summary; %v", err)
        }

        res, err := refreshLatestHandler(reqpath, &globals, ctx)
        if err != nil {
            t.Fatalf("failed to perform the refresh; %v", err)
        }
        if res.Version != "2" {
            t.Fatal("latest version is not as expected")
        }

        latest, err := readLatest(asset_dir)
        if err != nil {
            t.Fatalf("failed to read the latest version; %v", err)
        }
        if latest.Version != "2" {
            t.Fatal("latest version is not as expected after probation")
        }
    }

    // Making them all probational.
    {
        for i := 1; i <= 2; i++ {
            version_dir := filepath.Join(asset_dir, strconv.Itoa(i))
            summ, err := readSummary(version_dir)
            if err != nil {
                t.Fatalf("failed to read version %d; %v", i, err)
            }

            summ.OnProbation = &on_probation
            err = dumpJson(filepath.Join(version_dir, summaryFileName), &summ)
            if err != nil {
                t.Fatalf("failed to update version summary; %v", err)
            }
        }

        res, err := refreshLatestHandler(reqpath, &globals, ctx)
        if err != nil {
            t.Fatalf("failed to perform the refresh; %v", err)
        }
        if res != nil {
            t.Fatal("latest version should not exist for all-probational asset")
        }

        _, err = readLatest(asset_dir)
        if err == nil || !errors.Is(err, os.ErrNotExist) {
            t.Fatal("latest version should not exist for all-probational asset")
        }
    }
}
