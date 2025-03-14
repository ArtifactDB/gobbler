package main

import (
    "testing"
    "strings"
    "os"
    "path/filepath"
    "time"
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
        out, err := checkRequestFile("request-foo", staging, time.Minute)
        if err != nil {
            t.Fatal(err)
        }
        if out != filepath.Join(staging, "request-foo") {
            t.Fatalf("unexpected path %q", out)
        }
    })

    t.Run("name failure", func(t *testing.T) {
        _, err := checkRequestFile("foo", staging, time.Minute)
        if err == nil || !strings.Contains(err.Error(), "request-") {
            t.Fatal("should have failed")
        }
    })

    t.Run("locality failure", func(t *testing.T) {
        _, err := checkRequestFile("request-blah/../../foo", staging, time.Minute)
        if err == nil || !strings.Contains(err.Error(), "name of a file") {
            t.Fatal("should have failed")
        }
    })

    t.Run("not present", func(t *testing.T) {
        _, err := checkRequestFile("request-blah", staging, time.Minute)
        if err == nil || !strings.Contains(err.Error(), "failed to access") {
            t.Fatal("should have failed")
        }
    })

    err = os.Mkdir(filepath.Join(staging, "request-blah"), 0700)
    if err != nil {
        t.Fatal(err)
    }

    t.Run("directory", func(t *testing.T) {
        _, err := checkRequestFile("request-blah", staging, time.Minute)
        if err == nil || !strings.Contains(err.Error(), "directory") {
            t.Fatal("should have failed")
        }
    })

    err = os.Symlink(filepath.Join(staging, "request-foo"), filepath.Join(staging, "request-symlink"))
    if err != nil {
        t.Fatal(err)
    }

    t.Run("symlink", func(t *testing.T) {
        _, err := checkRequestFile("request-symlink", staging, time.Minute)
        if err == nil || !strings.Contains(err.Error(), "symbolic link") {
            t.Fatal("should have failed")
        }
    })

    err = os.Link(filepath.Join(staging, "request-foo"), filepath.Join(staging, "request-hardlink"))
    if err != nil {
        t.Fatal(err)
    }

    t.Run("hard link", func(t *testing.T) {
        _, err := checkRequestFile("request-hardlink", staging, time.Minute)
        if err == nil || !strings.Contains(err.Error(), "hard link") {
            t.Fatal("should have failed")
        }
    })

    err = os.Remove(filepath.Join(staging, "request-hardlink")) // removing the hardlink to test the rest.
    if err != nil {
        t.Fatal(err)
    }

    t.Run("expired", func(t *testing.T) {
        time.Sleep(time.Millisecond)
        _, err := checkRequestFile("request-foo", staging, 0)
        if err == nil || !strings.Contains(err.Error(), "expired") {
            t.Fatal("should have failed")
        }
    })
}

func TestActiveRequestRegistry(t *testing.T) {
    t.Run("basic", func(t *testing.T) {
        staging, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        a, err := newActiveRequestRegistry(staging, time.Millisecond * 200)

        path := "adasdasdasd"
        ok := a.Add(path)
        if !ok {
            t.Fatal("expected a successful addition")
        }

        ok = a.Add(path)
        if ok {
            t.Fatal("expected a failed addition")
        }

        time.Sleep(time.Millisecond * 500)
        ok = a.Add(path)
        if !ok {
            t.Fatal("expected a successful addition again")
        }

        ok = a.Add("xyxyxyxyxyx")
        if !ok {
            t.Fatal("expected a successful addition again")
        }
    })

    t.Run("preloaded", func(t *testing.T) {
        staging, err := os.MkdirTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        names := []string{ "foo", "bar", "whee" }
        for _, f := range names {
            err = os.WriteFile(filepath.Join(staging, f), []byte{}, 0644)
            if err != nil {
                t.Fatal(err)
            }
        }

        a, err := newActiveRequestRegistry(staging, time.Millisecond * 200)
        if err != nil {
            t.Fatal(err)
        }

        for _, f := range names {
            if a.Add(f) {
                t.Fatalf("%s should already be present in the registry", f)
            }
        }

        // Adding some more names.
        if !a.Add("stuff") {
            t.Fatal("failed to add some new names")
        }
        if a.Add("stuff") {
            t.Fatal("should have failed to add a duplicate name")
        }

        time.Sleep(time.Millisecond * 500)
        for _, f := range names {
            if !a.Add(f) {
                t.Fatalf("%s should have been removed from the registry", f)
            }
        }
    })
}
