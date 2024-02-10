package main

import (
    "testing"
    "os"
    "strings"
)

func TestReadUploadRequest(t *testing.T) {
    // Setting up the files.
    tmpfile, err := os.CreateTemp("", "")
    if err != nil {
        t.Fatalf("failed to create a temporary directory; %v", err)
    }
    tmpname := tmpfile.Name()
    tmpfile.Close()

    src, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create a temporary directory; %v", err)
    }

    // Simple check first.
    err = os.WriteFile(tmpname, []byte(`{ "source": "` + src + `", "project": "A", "version": "B", "asset": "C" }`), 0644)
    if err != nil {
        t.Fatalf("failed to dump the test JSON; %v", err)
    }
    req, err := ReadUploadRequest(tmpname)
    if err != nil {
        t.Fatalf("failed to parse the upload request; %v", err)
    }
    if *(req.Project) != "A" || *(req.Version) != "B" || *(req.Asset) != "C" || *(req.Source) != src || req.Prefix != nil || req.Permissions != nil {
        t.Fatalf("unexpected values from the upload request; %v", err)
    }

    // Trying with the prefix this time.
    err = os.WriteFile(tmpname, []byte(`{ "source": "` + src + `", "prefix": "A", "asset": "C" }`), 0644)
    if err != nil {
        t.Fatalf("failed to dump the test JSON; %v", err)
    }
    req, err = ReadUploadRequest(tmpname)
    if err != nil {
        t.Fatalf("failed to parse the upload request; %v", err)
    }
    if *(req.Prefix) != "A" || req.Version != nil || *(req.Asset) != "C" || *(req.Source) != src || req.Project != nil || req.Permissions != nil {
        t.Fatalf("unexpected values from the upload request")
    }

    // Adding some permissions.
    err = os.WriteFile(tmpname, []byte(`{ "source": "` + src + `", "project": "A", "asset": "C", "permissions": { "owners": [ "AARON" ] } }`), 0644)
    if err != nil {
        t.Fatalf("failed to dump the test JSON; %v", err)
    }
    req, err = ReadUploadRequest(tmpname)
    if err != nil {
        t.Fatalf("failed to parse the upload request; %v", err)
    }
    if req.Permissions == nil || req.Permissions.Owners == nil || len(req.Permissions.Owners) != 1 || req.Permissions.Owners[0] != "AARON" || req.Permissions.Uploaders != nil {
        t.Fatalf("unexpected values from the upload request; %v", err)
    }

    // Checking that the source directory is actually checked.
    err = os.WriteFile(tmpname, []byte(`{ "source": "` + src + `-missing", "project": "A", "asset": "C" }`), 0644)
    if err != nil {
        t.Fatalf("failed to dump the test JSON; %v", err)
    }
    _, err = ReadUploadRequest(tmpname)
    if err == nil || !strings.Contains(err.Error(), "failed to find owner") {
        t.Fatalf("failed to parse the upload request; %v", err)
    }
}
