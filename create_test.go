package main

import (
    "testing"
    "path/filepath"
    "fmt"
    "os/user"
    "strings"
)

func TestCreateProjectSimple(t *testing.T) {
    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg)

    self, err := user.Current()
    if err != nil {
        t.Fatalf("failed to determine the current user; %v", err)
    }
    globals.Administrators = append(globals.Administrators, self.Username)

    project := "foo"
    req_string := fmt.Sprintf(`{ "project": "%s"}`, project)
    reqname, err := dumpRequest("create_project", req_string)
    if err != nil {
        t.Fatalf("failed to create 'create_project' request; %v", err)
    }

    err = createProjectHandler(reqname, &globals)
    if err != nil {
        t.Fatalf("failed to create project; %v", err)
    }

    usage, err := readUsage(filepath.Join(reg, project))
    if err != nil {
        t.Fatalf("failed to read usage file; %v", err)
    }
    if usage.Total != 0 {
        t.Fatalf("usage should be zero for a newly created project; %v", err)
    }
}

func TestCreateProjectFailures(t *testing.T) {
    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg)

    {
        project := "foo"
        req_string := fmt.Sprintf(`{ "project": "%s"}`, project)
        reqname, err := dumpRequest("create_project", req_string)
        if err != nil {
            t.Fatalf("failed to create 'create_project' request; %v", err)
        }

        err = createProjectHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "not authorized") {
            t.Fatalf("creation should have failed without no permissions; %v", err)
        }
    }

    self, err := user.Current()
    if err != nil {
        t.Fatalf("failed to determine the current user; %v", err)
    }
    globals.Administrators = append(globals.Administrators, self.Username)

    {
        project := "..foo"
        req_string := fmt.Sprintf(`{ "project": "%s"}`, project)
        reqname, err := dumpRequest("create_project", req_string)
        if err != nil {
            t.Fatalf("failed to create 'create_project' request; %v", err)
        }

        err = createProjectHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "invalid project name") {
            t.Fatalf("creation should have failed with an invalid project; %v", err)
        }
    }

    {
        project := "foo"
        req_string := fmt.Sprintf(`{ "project": "%s"}`, project)
        reqname, err := dumpRequest("create_project", req_string)
        if err != nil {
            t.Fatalf("failed to create 'create_project' request; %v", err)
        }

        err = createProjectHandler(reqname, &globals)
        if err != nil {
            t.Fatalf("project creation failed; %v", err)
        }

        err = createProjectHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "already exists") {
            t.Fatalf("duplicate project creation should have failed; %v", err)
        }
    }
}

func TestCreateProjectNewPermissions(t *testing.T) {
    reg, err := constructMockRegistry()
    if err != nil {
        t.Fatalf("failed to create the registry; %v", err)
    }
    globals := newGlobalConfiguration(reg)

    self, err := user.Current()
    if err != nil {
        t.Fatalf("failed to determine the current user; %v", err)
    }
    globals.Administrators = append(globals.Administrators, self.Username)

    // Checking that owners are respected.
    {
        project := "indigo_league"
        perm_string := `{ "owners": [ "YAY", "NAY" ] }`
        req_string := fmt.Sprintf(`{ "project": "%s", "permissions": %s }`, project, perm_string)
        reqname, err := dumpRequest("create_project", req_string)
        if err != nil {
            t.Fatalf("failed to create 'create_project' request; %v", err)
        }

        err = createProjectHandler(reqname, &globals)
        if err != nil {
            t.Fatalf("failed to create a project; %v", err)
        }

        perms, err := readPermissions(filepath.Join(reg, project))
        if err != nil {
            t.Fatalf("failed to read the permissions; %v", err)
        }
        if len(perms.Owners) != 2 || perms.Owners[0] != "YAY" || perms.Owners[1] != "NAY" {
            t.Fatal("failed to set the owners correctly in the project permissions")
        }
        if len(perms.Uploaders) != 0 {
            t.Fatal("failed to set the uploaders correctly in the project permissions")
        }
    }

    {
        project := "johto_journeys"
        new_id := "foo"
        perm_string := fmt.Sprintf(`{ "uploaders": [ { "id": "%s" } ] }`, new_id)

        req_string := fmt.Sprintf(`{ "project": "%s", "permissions": %s }`, project, perm_string)
        reqname, err := dumpRequest("create_project", req_string)
        if err != nil {
            t.Fatalf("failed to create 'create_project' request; %v", err)
        }

        err = createProjectHandler(reqname, &globals)
        if err != nil {
            t.Fatalf("failed to create a project; %v", err)
        }

        perms, err := readPermissions(filepath.Join(reg, project))
        if err != nil {
            t.Fatalf("failed to read the permissions; %v", err)
        }
        if len(perms.Owners) != 1 { // switches to the creating user.
            t.Fatal("failed to set the owners correctly in the project permissions")
        }
        if len(perms.Uploaders) != 1 || perms.Uploaders[0].Id != new_id {
            t.Fatal("failed to set the uploaders correctly in the project permissions")
        }
    }

    // Check that uploaders in the permissions are validated.
    {
        project := "battle_frontier"
        req_string := fmt.Sprintf(`{ "project": "%s", "permissions": { "uploaders": [{}] } }`, project)
        reqname, err := dumpRequest("create_project", req_string)
        if err != nil {
            t.Fatalf("failed to create 'create_project' request; %v", err)
        }

        err = createProjectHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "invalid 'permissions.uploaders'") {
            t.Fatalf("expected project creation to fail from invalid 'uploaders'")
        }
    }

    {
        project := "sinnoh_league"
        perm_string := `{ "uploaders": [ { "id": "argle", "until": "bargle" } ] }`
        req_string := fmt.Sprintf(`{ "project": "%s", "permissions": %s }`, project, perm_string)
        reqname, err := dumpRequest("create_project", req_string)
        if err != nil {
            t.Fatalf("failed to create 'create_project' request; %v", err)
        }

        err = createProjectHandler(reqname, &globals)
        if err == nil || !strings.Contains(err.Error(), "invalid 'permissions.uploaders'") {
            t.Fatalf("expected project creation to fail from invalid 'uploaders'")
        }
    }
}
