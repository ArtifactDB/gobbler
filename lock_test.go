package main

import (
    "testing"
    "time"
    "strings"
)

func TestLock(t *testing.T) {
    pl := newPathLocks()

    path := "FOO"
    err := pl.LockPath(path, 10 * time.Second)
    if err != nil {
        t.Fatalf("failed to acquire the lock; %v", err)
    }

    err = pl.LockPath(path, 0 * time.Second)
    if err == nil || !strings.Contains(err.Error(), "timed out") {
        t.Fatal("should have failed to acquire the lock")
    }

    pl.UnlockPath(path)
    err = pl.LockPath(path, 0 * time.Second)
    if err != nil {
        t.Fatalf("failed to acquire the lock with a zero timeout; %v", err)
    }
}
