package main

import (
    "testing"
    "os"
    "path/filepath"
)

func TestReadManifest(t *testing.T) {
    f, err := os.MkdirTemp("", "test-")
    if err != nil {
        t.Fatalf("failed to create tempdir; %v", err)
    }

    err = os.WriteFile(
        filepath.Join(f, ManifestFileName),
        []byte(`
{ 
    "foobar": {
        "size": 10000,
        "md5sum": "abcdefgh"
    },
    "whee/stuff": {
        "size": 20000,
        "md5sum": "12345678",
        "link": {
            "project": "seabird",
            "asset": "albatross",
            "version": "987",
            "path": "genesis"
        }
    },
    "blah/boo/akira": {
        "size": 30000,
        "md5sum": "a1b2c3d4",
        "link": {
            "project": "evangelion",
            "asset": "ayanami",
            "version": "first",
            "path": "rei",
            "ancestor": {
                "project": "neon",
                "asset": "asuka",
                "version": "nerv",
                "path": "langley"
            }
        }
    }
}`),
        0644,
    )
    if err != nil {
        t.Fatalf("failed to create test manifest; %v", err)
    }

    out, err := ReadManifest(f)
    if err != nil {
        t.Fatalf("failed to read test manifest; %v", err)
    }

    if len(out) != 3 {
        t.Fatalf("unexpected length of manifest")
    }

    first, ok := out["foobar"]
    if !ok {
        t.Fatalf("expected 'foobar' to be in the manifest")
    }
    if first.Size != 10000 {
        t.Fatalf("unexpected size for 'foobar' in the manifest")
    }
    if first.Md5sum != "abcdefgh" {
        t.Fatalf("unexpected MD5 sum for 'foobar' in the manifest")
    }
    if first.Link != nil {
        t.Fatalf("unexpected links for 'foobar' in the manifest")
    }

    second, ok := out["whee/stuff"]
    if !ok {
        t.Fatalf("expected 'whee/stuff' to be in the manifest")
    }
    if second.Size != 20000 {
        t.Fatalf("unexpected size for 'whee/stuff' in the manifest")
    }
    if second.Md5sum != "12345678" {
        t.Fatalf("unexpected MD5 sum for 'whee/stuff' in the manifest")
    }
    if second.Link == nil {
        t.Fatalf("missing links for 'whee/stuff' in the manifest")
    }
    if second.Link.Project != "seabird" {
        t.Fatalf("unexpected link project for 'whee/stuff' in the manifest")
    }
    if second.Link.Asset != "albatross" {
        t.Fatalf("unexpected link asset for 'whee/stuff' in the manifest")
    }
    if second.Link.Version != "987" {
        t.Fatalf("unexpected link version for 'whee/stuff' in the manifest")
    }
    if second.Link.Path != "genesis" {
        t.Fatalf("unexpected link path for 'whee/stuff' in the manifest")
    }
    if second.Link.Ancestor != nil {
        t.Fatalf("unexpected link ancestor for 'whee/stuff' in the manifest")
    }

    third, ok := out["blah/boo/akira"]
    if !ok {
        t.Fatalf("expected 'blah/boo/akira' to be in the manifest")
    }
    if third.Size != 30000 {
        t.Fatalf("unexpected size for 'blah/boo/akira' in the manifest")
    }
    if third.Md5sum != "a1b2c3d4" {
        t.Fatalf("unexpected MD5 sum for 'blah/boo/akira' in the manifest")
    }
    if third.Link == nil {
        t.Fatalf("missing links for 'blah/boo/akira' in the manifest")
    }
    if third.Link.Project != "evangelion" {
        t.Fatalf("unexpected link project for 'blah/boo/akira' in the manifest")
    }
    if third.Link.Asset != "ayanami" {
        t.Fatalf("unexpected link asset for 'blah/boo/akira' in the manifest")
    }
    if third.Link.Version != "first" {
        t.Fatalf("unexpected link version for 'blah/boo/akira' in the manifest")
    }
    if third.Link.Path != "rei" {
        t.Fatalf("unexpected link path for 'blah/boo/akira' in the manifest")
    }
    if third.Link.Ancestor == nil {
        t.Fatalf("missing ancestor link for 'blah/boo/akira' in the manifest")
    }
    if third.Link.Ancestor.Project != "neon" {
        t.Fatalf("unexpected link ancestor project for 'blah/boo/akira' in the manifest")
    }
    if third.Link.Ancestor.Asset != "asuka" {
        t.Fatalf("unexpected link ancestor asset for 'blah/boo/akira' in the manifest")
    }
    if third.Link.Ancestor.Version != "nerv" {
        t.Fatalf("unexpected link ancestor version for 'blah/boo/akira' in the manifest")
    }
    if third.Link.Ancestor.Path != "langley" {
        t.Fatalf("unexpected link ancestor path for 'blah/boo/akira' in the manifest")
    }
}
