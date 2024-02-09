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

    lockpath := filepath.Join(dir, "LOCK")

    {
        handle, err := Lock(lockpath, 10 * time.Second)
        if err != nil {
            t.Fatalf("failed to create a lock file; %v", err)
        }

        if _, err := os.Stat(lockpath); err != nil {
            t.Fatalf("failed to create a lock file; %v", err)
        }

        err = Unlock(handle)
        if err != nil {
            t.Fatalf("failed to unlock the file; %v", err)
        }
    }

    {
        handle2, err := Lock(lockpath, 0)
        if err != nil {
            t.Fatalf("failed to use existing lock file; %v", err)
        }

        err = Unlock(handle2)
        if err != nil {
            t.Fatalf("failed to unlock the file; %v", err)
        }
    }
}
