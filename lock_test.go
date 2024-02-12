package main

import (
    "testing"
    "io/ioutil"
    "path/filepath"
    "time"
    "os"
)

func TestLock(t *testing.T) {
    dir, err := ioutil.TempDir("", "")
    if (err != nil) {
        t.Fatalf("failed to create a temporary directory; %v", err)
    }

    lockpath := filepath.Join(dir, lockFileName)

    {
        handle, err := lock(lockpath, 10 * time.Second)
        if err != nil {
            t.Fatalf("failed to create a lock file; %v", err)
        }

        if _, err := os.Stat(lockpath); err != nil {
            t.Fatalf("failed to create a lock file; %v", err)
        }

        err = unlock(handle)
        if err != nil {
            t.Fatalf("failed to unlock the file; %v", err)
        }
    }

    {
        handle2, err := lock(lockpath, 0)
        if err != nil {
            t.Fatalf("failed to use existing lock file; %v", err)
        }

        err = unlock(handle2)
        if err != nil {
            t.Fatalf("failed to unlock the file; %v", err)
        }
    }
}
