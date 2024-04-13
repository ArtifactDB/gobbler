package main

import (
    "testing"
    "strings"
    "os"
    "path/filepath"
)

func TestCheckRequestFile(t *testing.T) {
    staging, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatal(err)
    }

    err = os.WriteFile(filepath.Join(staging, "request-foo"), []byte("bar"), 0644)
    if err != nil {
        t.Fatal(err)
    }

    t.Run("success", func(t *testing.T) {
        out, err := checkRequestFile("request-foo", staging)
        if err != nil {
            t.Fatal(err)
        }
        if out != filepath.Join(staging, "request-foo") {
            t.Fatalf("unexpected path %q", out)
        }
    })

    t.Run("name failure", func(t *testing.T) {
        _, err := checkRequestFile("foo", staging)
        if err == nil || !strings.Contains(err.Error(), "request-") {
            t.Fatal("should have failed")
        }
    })

    t.Run("locality failure", func(t *testing.T) {
        _, err := checkRequestFile("request-blah/../../foo", staging)
        if err == nil || !strings.Contains(err.Error(), "local") {
            t.Fatal("should have failed")
        }
    })

    t.Run("not present", func(t *testing.T) {
        _, err := checkRequestFile("request-blah", staging)
        if err == nil || !strings.Contains(err.Error(), "failed to access") {
            t.Fatal("should have failed")
        }
    })

    err = os.Mkdir(filepath.Join(staging, "request-blah"), 0700)
    if err != nil {
        t.Fatal(err)
    }

    t.Run("directory", func(t *testing.T) {
        _, err := checkRequestFile("request-blah", staging)
        if err == nil || !strings.Contains(err.Error(), "directory") {
            t.Fatal("should have failed")
        }
    })

    err = os.Symlink(filepath.Join(staging, "request-foo"), filepath.Join(staging, "request-symlink"))
    if err != nil {
        t.Fatal(err)
    }

    t.Run("symlink", func(t *testing.T) {
        _, err := checkRequestFile("request-symlink", staging)
        if err == nil || !strings.Contains(err.Error(), "symbolic link") {
            t.Fatal("should have failed")
        }
    })

    err = os.Link(filepath.Join(staging, "request-foo"), filepath.Join(staging, "request-hardlink"))
    if err != nil {
        t.Fatal(err)
    }

    t.Run("hard link", func(t *testing.T) {
        _, err := checkRequestFile("request-hardlink", staging)
        if err == nil || !strings.Contains(err.Error(), "hard link") {
            t.Fatal("should have failed")
        }
    })
}

func TestActiveRegistry(t *testing.T) {
    a := newActiveRegistry(3)

    path := "adasdasdasd"
    ok := a.Add(path)
    if !ok {
        t.Fatal("expected a successful addition")
    }

    ok = a.Add(path)
    if ok {
        t.Fatal("expected a failed addition")
    }

    a.Remove(path)
    ok = a.Add(path)
    if !ok {
        t.Fatal("expected a successful addition again")
    }

    ok = a.Add("xyxyxyxyxyx")
    if !ok {
        t.Fatal("expected a successful addition again")
    }
}
