package main

import (
    "testing"
    "os"
    "os/user"
    "io/ioutil"
    "path/filepath"
    "time"
    "fmt"
    "strings"
)

func TestIdentifyUser(t *testing.T) {
    dir, err := ioutil.TempDir("", "")
    if (err != nil) {
        t.Fatalf("failed to create a temporary directory; %v", err)
    }

    username, err := IdentifyUser(dir)
    if err != nil {
        t.Fatalf("failed to identify user from file; %v", err)
    }

    self, err := user.Current()
    if err != nil {
        t.Fatalf("failed to identify current user; %v", err)
    }

    if username != self.Username {
        t.Fatalf("wrong user (expected + '" + self.Username + "', got '" + username + "')")
    }
}

func TestReadPermissions(t *testing.T) {
    f, err := os.MkdirTemp("", "test-")
    if err != nil {
        t.Fatalf("failed to create tempdir; %v", err)
    }

    err = os.WriteFile(filepath.Join(f, PermissionsFileName), []byte(`{ "owners": ["A", "B", "CC"], "uploaders": [ { "id": "excel" } ] }`), 0644)
    if err != nil {
        t.Fatalf("failed to create test ..latest; %v", err)
    }

    out, err := ReadPermissions(f)
    if err != nil {
        t.Fatalf("failed to read test ..latest; %v", err)
    }

    if out.Owners == nil || len(out.Owners) != 3 || out.Owners[0] != "A" || out.Owners[1] != "B" || out.Owners[2] != "CC" {
        t.Fatalf("unexpected 'owners' value")
    }

    if out.Uploaders == nil || len(out.Uploaders) != 1 || out.Uploaders[0].Id == nil || *(out.Uploaders[0].Id) != "excel" {
        t.Fatalf("unexpected 'uploaders' value")
    }
}

func TestIsAuthorized(t *testing.T) {
    perms := Permissions {
        Owners: []string{ "erika", "sabrina", "misty" },
        Uploaders: []Uploader{},
    }

    if IsAuthorizedToMaintain(&perms, "may") {
        t.Fatalf("unexpected authorization for non-owner")
    }

    if !IsAuthorizedToMaintain(&perms, "erika") {
        t.Fatalf("unexpected lack of authorization for owner")
    }

    ok, trusted := IsAuthorizedToUpload(&perms, "may", nil, nil)
    if ok {
        t.Fatalf("unexpected authorization for non-uploader")
    }

    ok, trusted = IsAuthorizedToUpload(&perms, "sabrina", nil, nil)
    if !ok || !trusted {
        t.Fatalf("unexpected lack of upload authorization for owner")
    }

    id1 := "may"
    id2 := "serena"
    perms.Uploaders = []Uploader{ Uploader{ Id: &id1 }, Uploader{ Id: &id2 } }
    ok, trusted = IsAuthorizedToUpload(&perms, "may", nil, nil)
    if !ok || trusted {
        t.Fatalf("unexpected lack of authorization for an uploader")
    }

    asset_name := "saffron"
    perms.Uploaders[1].Asset = &asset_name
    ok, trusted = IsAuthorizedToUpload(&perms, "serena", nil, nil)
    if ok {
        t.Fatalf("unexpected authorization for an uploader with no asset")
    }
    ok, trusted = IsAuthorizedToUpload(&perms, "serena", &asset_name, nil)
    if !ok || trusted {
        t.Fatalf("unexpected lack of authorization for an uploader with correct asset")
    }
    dummy_string := "pallet"
    ok, trusted = IsAuthorizedToUpload(&perms, "serena", &dummy_string, nil)
    if ok {
        t.Fatalf("unexpected authorization for an uploader with wrong asset")
    }

    version_name := "kanto"
    perms.Uploaders[1].Asset = nil
    perms.Uploaders[1].Version = &version_name
    ok, trusted = IsAuthorizedToUpload(&perms, "serena", nil, nil)
    if ok {
        t.Fatalf("unexpected authorization for an uploader with no version")
    }
    ok, trusted = IsAuthorizedToUpload(&perms, "serena", nil, &version_name)
    if !ok || trusted {
        t.Fatalf("unexpected lack of authorization for an uploader with correct version")
    }
    ok, trusted = IsAuthorizedToUpload(&perms, "serena", nil, &dummy_string)
    if ok {
        t.Fatalf("unexpected authorization for an uploader with wrong version")
    }

    perms.Uploaders[1].Version = nil
    bad_time := "AYYAYA"
    perms.Uploaders[1].Until = &bad_time
    ok, trusted = IsAuthorizedToUpload(&perms, "serena", nil, nil)
    if ok {
        t.Fatalf("unexpected authorization for an uploader with a bad time")
    }
    new_time := time.Now().Add(time.Hour).Format(time.RFC3339)
    perms.Uploaders[1].Until = &new_time
    ok, trusted = IsAuthorizedToUpload(&perms, "serena", nil, nil)
    if !ok || trusted {
        t.Fatalf("unexpected lack of authorization for an uploader with future time")
    }
    old_time := time.Now().Add(-time.Hour).Format(time.RFC3339)
    perms.Uploaders[1].Until = &old_time
    ok, trusted = IsAuthorizedToUpload(&perms, "serena", nil, nil)
    if ok {
        t.Fatalf("unexpected authorization for an uploader with expired time")
    }

    is_trusted := false
    perms.Uploaders[0].Trusted = &is_trusted
    ok, trusted = IsAuthorizedToUpload(&perms, "may", nil, nil)
    if !ok || trusted {
        t.Fatalf("unexpected lack of authorization for an uploader")
    }
    is_trusted = true
    ok, trusted = IsAuthorizedToUpload(&perms, "may", nil, nil)
    if !ok || !trusted {
        t.Fatalf("unexpected lack of non-probational authorization for an uploader")
    }
}

func TestValidateUploaders(t *testing.T) {
    id1 := "may"
    uploaders := []Uploader{ Uploader{ Id: &id1 }, Uploader{ Id: nil } }
    err := ValidateUploaders(uploaders)
    if err == nil || !strings.Contains(err.Error(), "should have an 'id'") {
        t.Fatal("validation of uploaders did not fail on 'id' check")
    }

    id2 := "serena"
    uploaders[1].Id = &id2
    err = ValidateUploaders(uploaders)
    if err != nil {
        t.Fatalf("validation of uploaders failed for correct uploaders; %v", err)
    }

    mock := "YAAY"
    uploaders[1].Until = &mock
    err = ValidateUploaders(uploaders)
    if err == nil || !strings.Contains(err.Error(), "Internet Date/Time") {
        t.Fatal("validation of uploaders did not fail for invalid 'until'")
    }

    mock = time.Now().Format(time.RFC3339)
    err = ValidateUploaders(uploaders)
    if err != nil {
        t.Fatal("validation of uploaders failed for valid 'until'")
    }
}

func TestSetPermissions(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    self, err := IdentifyUser(reg)
    if err != nil {
        t.Fatalf("failed to identify self; %v", err)
    }

    project := "cynthia"
    project_dir := filepath.Join(reg, project)
    err = os.Mkdir(project_dir, 0755)
    if err != nil {
        t.Fatalf("failed to create a project directory; %v", err)
    }

    err = os.WriteFile(
        filepath.Join(project_dir, PermissionsFileName),
        []byte(fmt.Sprintf(`
{
    "owners": [ "brock", "ash", "oak", "%s" ],
    "uploaders": [ { "id": "lance" } ]
}
        `, self)),
        0644,
    )
    if err != nil {
        t.Fatalf("failed to create some mock permissions; %v", err)
    }

    {
        reqpath, err := dump_request(
            "permissions",
            fmt.Sprintf(`{ "project": "%s", "permissions": { "owners": [ "%s", "gary" ] } }`, project, self),
        )
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        err = SetPermissions(reqpath, reg)
        if err != nil {
            t.Fatalf("failed to set permissions; %v", err)
        }

        perms, err := ReadPermissions(project_dir)
        if err != nil {
            t.Fatalf("failed to read the new permissions; %v", err)
        }

        if perms.Owners == nil || len(perms.Owners) != 2 || perms.Owners[0] != self || perms.Owners[1] != "gary" {
            t.Fatal("owners were not modified as expected")
        }
        if perms.Uploaders == nil || len(perms.Uploaders) != 1 || *(perms.Uploaders[0].Id) != "lance" {
            t.Fatal("uploaders were not preserved as expected")
        }
    }

    {
        reqpath, err := dump_request(
            "permissions",
            fmt.Sprintf(`
{ 
    "project": "%s", 
    "permissions": { 
        "uploaders": [ 
            { "id": "lorelei", "until": "2022-02-02T20:20:20Z" },
            { "id": "karen" }
        ]
    }
}`, project),
        )
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        err = SetPermissions(reqpath, reg)
        if err != nil {
            t.Fatalf("failed to set permissions; %v", err)
        }

        perms, err := ReadPermissions(project_dir)
        if err != nil {
            t.Fatalf("failed to read the new permissions; %v", err)
        }

        if perms.Owners == nil || len(perms.Owners) != 2 {
            t.Fatal("owners were not preservfed as expected")
        }
        if perms.Uploaders == nil || len(perms.Uploaders) != 2 || *(perms.Uploaders[0].Id) != "lorelei" || perms.Uploaders[0].Until == nil || *(perms.Uploaders[1].Id) != "karen" {
            t.Fatal("uploaders were not preserved as expected")
        }
    }

    {
        err = os.WriteFile(
            filepath.Join(project_dir, PermissionsFileName),
            []byte(`
{
    "owners": [ "brock" ],
    "uploaders": [ { "id": "lance" } ]
}
            `),
            0644,
        )
        if err != nil {
            t.Fatalf("failed to create some mock permissions")
        }

        reqpath, err := dump_request(
            "permissions",
            fmt.Sprintf(`{ "project": "%s" }`, project),
        )
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        err = SetPermissions(reqpath, reg)
        if err == nil || !strings.Contains(err.Error(), "not authorized") {
            t.Fatalf("unexpected authorization for a non-owner")
        }
    }
}
