package main

import (
    "testing"
    "time"
    "strings"
    "os"
)

func TestLock(t *testing.T) {
    path, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create a mock directory; %v", err)
    }

    pl := newPathLocks()
    err = pl.LockDirectory(path, 10 * time.Second)
    if err != nil {
        t.Fatalf("failed to acquire the lock; %v", err)
    }

    err = pl.LockDirectory(path, 0 * time.Second)
    if err == nil || !strings.Contains(err.Error(), "timed out") {
        t.Fatal("should have failed to acquire the lock")
    }

    pl.Unlock(path)
    err = pl.LockDirectory(path, 0 * time.Second)
    if err != nil {
        t.Fatalf("failed to acquire the lock with a zero timeout; %v", err)
    }
}
