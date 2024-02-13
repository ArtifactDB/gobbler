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

    username, err := identifyUser(dir)
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

    err = os.WriteFile(filepath.Join(f, permissionsFileName), []byte(`{ "owners": ["A", "B", "CC"], "uploaders": [ { "id": "excel" } ] }`), 0644)
    if err != nil {
        t.Fatalf("failed to create test ..latest; %v", err)
    }

    out, err := readPermissions(f)
    if err != nil {
        t.Fatalf("failed to read test ..latest; %v", err)
    }

    if out.Owners == nil || len(out.Owners) != 3 || out.Owners[0] != "A" || out.Owners[1] != "B" || out.Owners[2] != "CC" {
        t.Fatalf("unexpected 'owners' value")
    }

    if out.Uploaders == nil || len(out.Uploaders) != 1 || out.Uploaders[0].Id != "excel" {
        t.Fatalf("unexpected 'uploaders' value")
    }
}

func TestIsAuthorizedToAdmin(t *testing.T) {
    if isAuthorizedToAdmin("may", []string{"erika"}) {
        t.Fatalf("unexpected authorization for non-admin")
    }
    if isAuthorizedToAdmin("may", nil) {
        t.Fatalf("unexpected authorization for non-admin")
    }
    if !isAuthorizedToAdmin("erika", []string{"erika"}) {
        t.Fatalf("unexpected lack of authorization for admin")
    }
}

func TestIsAuthorizedToMaintain(t *testing.T) {
    owners := []string{ "erika", "sabrina", "misty" }

    if isAuthorizedToMaintain("may", nil, owners) {
        t.Fatalf("unexpected authorization for non-owner")
    }
    if isAuthorizedToMaintain("may", nil, nil) {
        t.Fatalf("unexpected authorization for non-owner")
    }
    if !isAuthorizedToMaintain("erika", nil, owners) {
        t.Fatalf("unexpected lack of authorization for owner")
    }
    if !isAuthorizedToMaintain("may", []string{"may"}, owners) {
        t.Fatalf("unexpected lack of authorization for admin")
    }
}

func TestIsAuthorizedToUpload(t *testing.T) {
    perms := permissionsMetadata {
        Owners: []string{ "erika", "sabrina", "misty" },
        Uploaders: []uploaderEntry{},
    }

    ok, trusted := isAuthorizedToUpload("may", nil, &perms, nil, nil)
    if ok {
        t.Fatalf("unexpected authorization for non-uploader")
    }
    ok, trusted = isAuthorizedToUpload("may", []string{ "may" }, &perms, nil, nil)
    if !ok || !trusted {
        t.Fatalf("unexpected lack of authorization for an admin")
    }

    ok, trusted = isAuthorizedToUpload("sabrina", nil, &perms, nil, nil)
    if !ok || !trusted {
        t.Fatalf("unexpected lack of upload authorization for owner")
    }

    perms.Uploaders = []uploaderEntry{ uploaderEntry{ Id: "may" }, uploaderEntry{ Id: "serena" } }
    ok, trusted = isAuthorizedToUpload("may", nil, &perms, nil, nil)
    if !ok || trusted {
        t.Fatalf("unexpected lack of authorization for an uploader")
    }

    asset_name := "saffron"
    perms.Uploaders[1].Asset = &asset_name
    ok, trusted = isAuthorizedToUpload("serena", nil, &perms, nil, nil)
    if ok {
        t.Fatalf("unexpected authorization for an uploader with no asset")
    }
    ok, trusted = isAuthorizedToUpload("serena", nil, &perms, &asset_name, nil)
    if !ok || trusted {
        t.Fatalf("unexpected lack of authorization for an uploader with correct asset")
    }
    dummy_string := "pallet"
    ok, trusted = isAuthorizedToUpload("serena", nil, &perms, &dummy_string, nil)
    if ok {
        t.Fatalf("unexpected authorization for an uploader with wrong asset")
    }

    version_name := "kanto"
    perms.Uploaders[1].Asset = nil
    perms.Uploaders[1].Version = &version_name
    ok, trusted = isAuthorizedToUpload("serena", nil, &perms, nil, nil)
    if ok {
        t.Fatalf("unexpected authorization for an uploader with no version")
    }
    ok, trusted = isAuthorizedToUpload("serena", nil, &perms, nil, &version_name)
    if !ok || trusted {
        t.Fatalf("unexpected lack of authorization for an uploader with correct version")
    }
    ok, trusted = isAuthorizedToUpload("serena", nil, &perms, nil, &dummy_string)
    if ok {
        t.Fatalf("unexpected authorization for an uploader with wrong version")
    }

    perms.Uploaders[1].Version = nil
    bad_time := "AYYAYA"
    perms.Uploaders[1].Until = &bad_time
    ok, trusted = isAuthorizedToUpload("serena", nil, &perms, nil, nil)
    if ok {
        t.Fatalf("unexpected authorization for an uploader with a bad time")
    }
    new_time := time.Now().Add(time.Hour).Format(time.RFC3339)
    perms.Uploaders[1].Until = &new_time
    ok, trusted = isAuthorizedToUpload("serena", nil, &perms, nil, nil)
    if !ok || trusted {
        t.Fatalf("unexpected lack of authorization for an uploader with future time")
    }
    old_time := time.Now().Add(-time.Hour).Format(time.RFC3339)
    perms.Uploaders[1].Until = &old_time
    ok, trusted = isAuthorizedToUpload("serena", nil, &perms, nil, nil)
    if ok {
        t.Fatalf("unexpected authorization for an uploader with expired time")
    }

    is_trusted := false
    perms.Uploaders[0].Trusted = &is_trusted
    ok, trusted = isAuthorizedToUpload("may", nil, &perms, nil, nil)
    if !ok || trusted {
        t.Fatalf("unexpected lack of authorization for an uploader")
    }
    is_trusted = true
    ok, trusted = isAuthorizedToUpload("may", nil, &perms, nil, nil)
    if !ok || !trusted {
        t.Fatalf("unexpected lack of non-probational authorization for an uploader")
    }
}

func TestSanitizeUploaders(t *testing.T) {
    id1 := "may"
    uploaders := []unsafeUploaderEntry{ unsafeUploaderEntry{ Id: &id1 }, unsafeUploaderEntry{ Id: nil } }
    _, err := sanitizeUploaders(uploaders)
    if err == nil || !strings.Contains(err.Error(), "should have an 'id'") {
        t.Fatal("validation of uploaders did not fail on 'id' check")
    }

    id2 := "serena"
    uploaders[1].Id = &id2
    san, err := sanitizeUploaders(uploaders)
    if err != nil || len(san) != 2 || san[0].Id != id1 || san[1].Id != id2 {
        t.Fatalf("validation of uploaders failed for correct uploaders; %v", err)
    }

    mock := "YAAY"
    uploaders[1].Until = &mock
    _, err = sanitizeUploaders(uploaders)
    if err == nil || !strings.Contains(err.Error(), "Internet Date/Time") {
        t.Fatal("validation of uploaders did not fail for invalid 'until'")
    }

    mock = time.Now().Format(time.RFC3339)
    san, err = sanitizeUploaders(uploaders)
    if err != nil || len(san) != 2 || san[1].Until == nil || *(san[1].Until) != mock {
        t.Fatal("validation of uploaders failed for valid 'until'")
    }
}

func TestSetPermissionsHandlerHandler(t *testing.T) {
    reg, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }

    self, err := identifyUser(reg)
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
        filepath.Join(project_dir, permissionsFileName),
        []byte(fmt.Sprintf(`{ "owners": [ "brock", "ash", "oak", "%s" ], "uploaders": [ { "id": "lance" } ] }`, self)),
        0644,
    )
    if err != nil {
        t.Fatalf("failed to create some mock permissions; %v", err)
    }

    // Pure owners.
    {
        reqpath, err := dumpRequest(
            "set_permissions",
            fmt.Sprintf(`{ "project": "%s", "permissions": { "owners": [ "%s", "gary" ] } }`, project, self),
        )
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        err = setPermissionsHandler(reqpath, reg, nil)
        if err != nil {
            t.Fatalf("failed to set permissions; %v", err)
        }

        perms, err := readPermissions(project_dir)
        if err != nil {
            t.Fatalf("failed to read the new permissions; %v", err)
        }

        if perms.Owners == nil || len(perms.Owners) != 2 || perms.Owners[0] != self || perms.Owners[1] != "gary" {
            t.Fatal("owners were not modified as expected")
        }
        if perms.Uploaders == nil || len(perms.Uploaders) != 1 || perms.Uploaders[0].Id != "lance" {
            t.Fatal("uploaders were not preserved as expected")
        }
    }

    // Pure uploaders.
    {
        reqpath, err := dumpRequest(
            "set_permissions",
            fmt.Sprintf(`{ "project": "%s", "permissions": { "uploaders": [ { "id": "lorelei", "until": "2022-02-02T20:20:20Z" }, { "id": "karen" } ] } }`, project),
        )
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        err = setPermissionsHandler(reqpath, reg, nil)
        if err != nil {
            t.Fatalf("failed to set permissions; %v", err)
        }

        perms, err := readPermissions(project_dir)
        if err != nil {
            t.Fatalf("failed to read the new permissions; %v", err)
        }

        if perms.Owners == nil || len(perms.Owners) != 2 {
            t.Fatal("owners were not preservfed as expected")
        }
        if perms.Uploaders == nil || len(perms.Uploaders) != 2 || perms.Uploaders[0].Id != "lorelei" || perms.Uploaders[0].Until == nil || perms.Uploaders[1].Id != "karen" {
            t.Fatal("uploaders were not preserved as expected")
        }
    }

    // Invalid uploaders.
    {
        reqpath, err := dumpRequest(
            "set_permissions",
            fmt.Sprintf(`{ "project": "%s", "permissions": { "uploaders": [ { } ] } }`, project),
        )
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        err = setPermissionsHandler(reqpath, reg, nil)
        if err == nil || !strings.Contains(err.Error(), "invalid 'permissions.uploaders'") {
            t.Fatal("expected a permissions failure for invalid uploaders")
        }

        reqpath, err = dumpRequest(
            "set_permissions",
            fmt.Sprintf(`{ "project": "%s", "permissions": { "uploaders": [ { "id": "cynthia", "until": "YAY" } ] } }`, project),
        )
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        err = setPermissionsHandler(reqpath, reg, nil)
        if err == nil || !strings.Contains(err.Error(), "invalid 'permissions.uploaders'") {
            t.Fatal("expected a permissions failure for invalid uploaders")
        }
    }

    // Not authorized.
    {
        err = os.WriteFile(
            filepath.Join(project_dir, permissionsFileName),
            []byte(`{ "owners": [ "brock" ], "uploaders": [ { "id": "lance" } ] } `),
            0644,
        )
        if err != nil {
            t.Fatalf("failed to create some mock permissions")
        }

        reqpath, err := dumpRequest(
            "set_permissions",
            fmt.Sprintf(`{ "project": "%s" }`, project),
        )
        if err != nil {
            t.Fatalf("failed to dump a request type; %v", err)
        }

        err = setPermissionsHandler(reqpath, reg, nil)
        if err == nil || !strings.Contains(err.Error(), "not authorized") {
            t.Fatalf("unexpected authorization for a non-owner")
        }
    }
}
