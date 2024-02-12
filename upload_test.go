package main

import (
    "testing"
    "os"
    "path/filepath"
    "fmt"
    "os/user"
    "time"
    "errors"
)

func setup_source_for_upload_test() (string, error) {
    src, err := os.MkdirTemp("", "")
    if err != nil {
        return "", fmt.Errorf("failed to create the temporary directory; %w", err)
    }

    err = os.WriteFile(filepath.Join(src, "evolution"), []byte("haunter"), 0644)
    if err != nil {
        return "", err
    }

    err = os.WriteFile(filepath.Join(src, "moves"), []byte("lick,confuse_ray,shadow_ball,dream_eater"), 0644)
    if err != nil {
        return "", err
    }

    return src, nil
}

func TestUploadSimple(t *testing.T) {
    project := "original_series"
    asset := "gastly"

    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    src, err := setup_source_for_upload_test()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    req_string := fmt.Sprintf(`{ "source": "%s", "project": "%s", "asset": "%s" }`, src, project, asset)
    reqname, err := dumpRequest("upload", req_string)
    if err != nil {
        t.Fatalf("failed to create upload request; %v", err)
    }

    // Executing the first transfer.
    old_usage := int64(0)
    {
        config, err := Upload(reqname, reg, nil)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }
        if config.Project != project {
            t.Fatalf("unexpected project name %q", config.Project)
        }
        if config.Asset != asset {
            t.Fatalf("unexpected asset name %q", config.Asset)
        }
        if config.Version != "1" {
            t.Fatalf("unexpected version name %q", config.Version)
        }

        // Checking a few manifest entries and files.
        destination := filepath.Join(reg, config.Project, config.Asset, config.Version)
        man, err := ReadManifest(destination)
        if err != nil {
            t.Fatalf("failed to read the manifest; %v", err)
        }
        info, ok := man["evolution"]
        if !ok || int(info.Size) != len("haunter") || info.Link != nil {
            t.Fatal("unexpected manifest entry for 'evolution'")
        }
        err = verify_file_contents(filepath.Join(destination, "moves"), "lick,confuse_ray,shadow_ball,dream_eater")
        if err != nil {
            t.Fatalf("could not verify 'moves'; %v", err)
        }

        // Checking out the summary.
        summ, err := ReadSummary(destination)
        if err != nil {
            t.Fatalf("failed to read the summary; %v", err)
        }

        self, err := user.Current()
        if err != nil {
            t.Fatalf("failed to determine the current user; %v", err)
        }
        if summ.UploadUserId != self.Username {
            t.Fatalf("user in summary is not as expected (expected %q, got %q)", self.Username, summ.UploadUserId)
        }

        ustart, err := time.Parse(time.RFC3339, summ.UploadStart)
        if err != nil {
            t.Fatalf("upload start is not a valid time; %v", err)
        }
        ufinish, err := time.Parse(time.RFC3339, summ.UploadFinish)
        if err != nil {
            t.Fatalf("upload finish is not a valid time; %v", err)
        }
        if ustart.After(ufinish) {
            t.Fatalf("upload finish should be at or after the upload start; %v", err)
        }

        if summ.OnProbation != nil {
            t.Fatal("no probation property should be present")
        }

        // Checking out the usage.
        project_dir := filepath.Join(reg, config.Project)
        used, err := ReadUsage(project_dir)
        if err != nil {
            t.Fatalf("failed to read the usage; %v", err)
        }
        expected_usage, err := ComputeUsage(project_dir, true)
        if err != nil {
            t.Fatalf("failed to compute the expected usage; %v", err)
        }
        if expected_usage != used.Total {
            t.Fatalf("unexpected usage total (expected %d, got %d)", expected_usage, used.Total)
        }
        manifest_sum := int64(0)
        for _, m := range man {
            manifest_sum += m.Size
        }
        if expected_usage != manifest_sum {
            t.Fatalf("usage total does not match with sum of sizes in manifest (expected %d, got %d)", expected_usage, manifest_sum)
        }
        old_usage = expected_usage

        // Checking out the latest version.
        latest, err := ReadLatest(filepath.Join(reg, config.Project, config.Asset))
        if err != nil {
            t.Fatalf("failed to read the latest; %v", err)
        }
        if latest.Latest != config.Version {
            t.Fatalf("unexpected latest version (expected %q, got %q)", latest.Latest, config.Version)
        }
    }

    // Executing another transfer on a different version.
    {
        all_evos := "haunter,gengar"
        err = os.WriteFile(filepath.Join(src, "evolution"), []byte(all_evos), 0644)
        if err != nil {
            t.Fatalf("failed to update the 'evolution' file; %v", err)
        }

        config, err := Upload(reqname, reg, nil)
        if err != nil {
            t.Fatalf("failed to perform the upload; %v", err)
        }
        if config.Asset != asset {
            t.Fatalf("unexpected asset name %q", config.Asset)
        }
        if config.Version != "2" {
            t.Fatalf("unexpected version name %q", config.Version)
        }

        destination := filepath.Join(reg, config.Project, config.Asset, config.Version)
        man, err := ReadManifest(destination)
        if err != nil {
            t.Fatalf("failed to read the manifest; %v", err)
        }
        info, ok := man["evolution"]
        if !ok || int(info.Size) != len(all_evos) || info.Link != nil {
            t.Fatal("unexpected manifest entry for 'evolution'")
        }
        minfo, ok := man["moves"]
        if !ok || minfo.Link == nil {
            t.Fatal("expected a link for 'moves' in the manifest")
        }
        err = verify_file_contents(filepath.Join(destination, "evolution"), all_evos)
        if err != nil {
            t.Fatalf("could not verify 'evolution'; %v", err)
        }

        // Ensuring that the usage accumulates.
        project_dir := filepath.Join(reg, config.Project)
        usage, err := ReadUsage(project_dir)
        if err != nil {
            t.Fatalf("failed to read the usage; %v", err)
        }
        expected_usage, err := ComputeUsage(project_dir, true)
        if err != nil {
            t.Fatalf("failed to compute the expected usage; %v", err)
        }
        if expected_usage != usage.Total {
            t.Fatalf("unexpected usage total (expected %d, got %d)", expected_usage, usage.Total)
        }
        for _, m := range man {
            if m.Link == nil {
                old_usage += m.Size
            }
        }
        if usage.Total != old_usage {
            t.Fatalf("usage total should equal the sum of non-link sizes (expected %d, got %d)", old_usage, usage.Total)
        }

        // Confirming that we updated to the latest version.
        latest, err := ReadLatest(filepath.Join(reg, config.Project, config.Asset))
        if err != nil {
            t.Fatalf("failed to read the latest; %v", err)
        }
        if latest.Latest != config.Version {
            t.Fatalf("unexpected latest version (expected %q, got %q)", config.Version, latest.Latest)
        }
    }
}

func TestUploadProbation(t *testing.T) {
    prefix := "POKEDEX"
    asset := "Gastly"

    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    src, err := setup_source_for_upload_test()
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    req_string := fmt.Sprintf(`{ "source": "%s", "prefix": "%s", "asset": "%s", "version": "FOO", "on_probation": true }`, src, prefix, asset)
    reqname, err := dumpRequest("upload", req_string)
    if err != nil {
        t.Fatalf("failed to create upload request; %v", err)
    }

    config, err := Upload(reqname, reg, nil)
    if err != nil {
        t.Fatalf("failed to perform the upload; %v", err)
    }
    if config.Project != "POKEDEX1" {
        t.Fatalf("unexpected project name %q", config.Project)
    }
    if config.Asset != asset {
        t.Fatalf("unexpected asset name %q", config.Asset)
    }
    if config.Version != "FOO" {
        t.Fatalf("unexpected version name %q", config.Version)
    }

    // Summary file states that it's on probation.
    summ, err := ReadSummary(filepath.Join(reg, config.Project, config.Asset, config.Version))
    if err != nil {
        t.Fatalf("failed to read the summary; %v", err)
    }
    if !summ.IsProbational() {
        t.Fatal("expected version to be on probation")
    }

    // No latest file should be created for probational projects.
    _, err = ReadLatest(filepath.Join(reg, config.Project, config.Asset))
    if err == nil || !errors.Is(err, os.ErrNotExist) {
        t.Fatal("no ..latest file should be created on probation")
    }
}
