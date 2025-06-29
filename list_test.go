package main

import (
    "testing"
    "os"
    "path/filepath"
    "sort"
    "strings"
    "net/http"
    "context"
)

func TestListFiles(t *testing.T) {
    simulate := func(dir string) {
        path := filepath.Join(dir, "A")
        err := os.WriteFile(path, []byte(""), 0644)
        if err != nil {
            t.Fatalf("failed to create a mock file; %v", err)
        }

        subdir := filepath.Join(dir, "sub")
        err = os.Mkdir(subdir, 0755)
        if err != nil {
            t.Fatalf("failed to create a temporary subdirectory; %v", err)
        }

        subpath := filepath.Join(subdir, "B")
        err = os.WriteFile(subpath, []byte(""), 0644)
        if err != nil {
            t.Fatalf("failed to create a mock file; %v", err)
        }
    }

    ctx := context.Background()

    t.Run("simple", func(t *testing.T) {
        dir, err := os.MkdirTemp("", "")
        if (err != nil) {
            t.Fatalf("failed to create a temporary directory; %v", err)
        }
        simulate(dir)

        // Checking that we pull out all the files.
        all, err := listFiles(dir, true, ctx)
        if (err != nil) {
            t.Fatal(err)
        }

        sort.Strings(all)
        if len(all) != 2 || all[0] != "A" || all[1] != "sub/B" {
            t.Errorf("unexpected results from the listing (%q)", all)
        }

        // Checking that the directories are properly listed.
        all, err = listFiles(dir, false, ctx)
        if (err != nil) {
            t.Fatal(err)
        }

        sort.Strings(all)
        if len(all) != 2 || all[0] != "A" || all[1] != "sub/" {
            t.Errorf("unexpected results from the listing (%q)", all)
        }

        // Checking that cancellation works as expected.
        expired, _ := context.WithTimeout(context.Background(), 0)
        _, err = listFiles(dir, true, expired)
        if err == nil || !strings.Contains(err.Error(), "cancelled") {
            t.Errorf("expected an error from a cancelled context")
        }
    })

    t.Run("empty", func(t *testing.T) {
        dir, err := os.MkdirTemp("", "")
        if (err != nil) {
            t.Fatalf("failed to create a temporary directory; %v", err)
        }
        simulate(dir)

        // Spiking in some empty directories.
        subdir := filepath.Join(dir, "stuff")
        err = os.Mkdir(subdir, 0755)
        if err != nil {
            t.Fatalf("failed to create a temporary subdirectory; %v", err)
        }

        subdir = filepath.Join(dir, "sub", "whee")
        err = os.Mkdir(subdir, 0755)
        if err != nil {
            t.Fatalf("failed to create a temporary subdirectory; %v", err)
        }

        subdir = filepath.Join(dir, "foo", "bar")
        err = os.MkdirAll(subdir, 0755)
        if err != nil {
            t.Fatalf("failed to create a temporary subdirectory; %v", err)
        }

        // Recursive search preserves the empty directories.
        all, err := listFiles(dir, true, ctx)
        if (err != nil) {
            t.Fatal(err)
        }

        sort.Strings(all)
        if len(all) != 5 || all[0] != "A" || all[1] != "foo/bar/" || all[2] != "stuff/" || all[3] != "sub/B" || all[4] != "sub/whee/" {
            t.Errorf("unexpected results from the listing (%q)", all)
        }

        // As does the non-recursive search.
        all, err = listFiles(dir, false, ctx)
        if (err != nil) {
            t.Fatal(err)
        }

        sort.Strings(all)
        if len(all) != 4 || all[0] != "A" || all[1] != "foo/" || all[2] != "stuff/" || all[3] != "sub/" {
            t.Errorf("unexpected results from the listing (%q)", all)
        }
    })

    t.Run("handler", func(t *testing.T) {
        dir, err := os.MkdirTemp("", "")
        if (err != nil) {
            t.Fatalf("failed to create a temporary directory; %v", err)
        }
        simulate(dir)

        {
            r, err := http.NewRequest("GET", "/list?path=sub", nil) 
            if err != nil {
                t.Fatal(err)
            }

            all, err := listFilesHandler(r, dir)
            if (err != nil) {
                t.Fatal(err)
            }

            if len(all) != 1 || all[0] != "B" {
                t.Errorf("unexpected results from the listing (%q)", all)
            }
        }

        {
            r, err := http.NewRequest("GET", "/list?recursive=true", nil) 
            if err != nil {
                t.Fatal(err)
            }

            all, err := listFilesHandler(r, dir)
            if (err != nil) {
                t.Fatal(err)
            }

            sort.Strings(all)
            if len(all) != 2 || all[0] != "A" || all[1] != "sub/B" {
                t.Errorf("unexpected file results from the listing (%q)", all)
            }
        }

        {
            r, err := http.NewRequest("GET", "/list?path=..", nil) 
            if err != nil {
                t.Fatal(err)
            }

            _, err = listFilesHandler(r, dir)
            if err == nil || !strings.Contains(err.Error(), "not local") {
                t.Fatal("expected failure for non-local paths")
            }
        }

        {
            r, err := http.NewRequest("GET", "/list?path=sub%2F", nil) 
            if err != nil {
                t.Fatal(err)
            }

            all, err := listFilesHandler(r, dir)
            if err != nil {
                t.Fatal(err)
            }

            if len(all) != 1 || all[0] != "B" {
                t.Errorf("unexpected file results from the listing (%q)", all)
            }
        }
    })
}

func TestListUserDirectories(t *testing.T) {
    dir, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatal(err)
    }

    err = os.Mkdir(filepath.Join(dir, "foo"), 0755)
    if err != nil {
        t.Fatal(err)
    }

    err = os.Mkdir(filepath.Join(dir, "bar"), 0755)
    if err != nil {
        t.Fatal(err)
    }

    err = os.Mkdir(filepath.Join(dir, logDirName), 0755)
    if err != nil {
        t.Fatal(err)
    }

    err = os.WriteFile(filepath.Join(dir, "foo", "whee"), []byte{}, 0644)
    if err != nil {
        t.Fatal(err)
    }

    err = os.WriteFile(filepath.Join(dir, "stuff"), []byte{}, 0644)
    if err != nil {
        t.Fatal(err)
    }

    available, err := listUserDirectories(dir)
    if err != nil {
        t.Fatal(err)
    }

    sort.Strings(available)
    if len(available) != 2 || available[0] != "bar" || available[1] != "foo" {
        t.Errorf("unexpected listing of user directories: %v", available)
    }
}
