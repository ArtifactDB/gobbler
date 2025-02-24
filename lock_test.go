package main

import (
    "testing"
    "time"
    "strings"
    "os"
    "sync"
)

func TestLock(t *testing.T) {
    path, err := os.MkdirTemp("", "")
    if err != nil {
        t.Fatalf("failed to create a mock directory; %v", err)
    }

    t.Run("exclusive", func(t *testing.T) {
        pl := newPathLocks()
        err = pl.Lock(path, 10 * time.Second, true)
        if err != nil {
            t.Fatalf("failed to acquire the lock; %v", err)
        }

        err = pl.Lock(path, 0 * time.Second, true)
        if err == nil || !strings.Contains(err.Error(), "timed out") {
            t.Fatal("should have failed to acquire another exclusive lock")
        }

        err = pl.Lock(path, 0 * time.Second, false)
        if err == nil || !strings.Contains(err.Error(), "timed out") {
            t.Fatal("should have failed to acquire a shared lock")
        }

        pl.Unlock(path)
        err = pl.Lock(path, 0 * time.Second, true)
        if err != nil {
            t.Fatalf("failed to acquire the lock with a zero timeout; %v", err)
        }

        pl.Unlock(path)
    })

    t.Run("shared", func(t *testing.T) {
        pl := newPathLocks()
        err = pl.Lock(path, 10 * time.Second, false)
        if err != nil {
            t.Fatalf("failed to acquire the lock; %v", err)
        }

        err = pl.Lock(path, 0 * time.Second, false)
        if err != nil {
            t.Fatalf("failed to acquire the lock with a zero timeout; %v", err)
        }

        // Can't acquire another exclusive lock.
        err = pl.Lock(path, 10 * time.Millisecond, true)
        if err == nil || !strings.Contains(err.Error(), "timed out") {
            t.Errorf("should have failed to acquire an exclusive lock")
        }

        // Still can't acquire an exclusive lock until all shared locks are released.
        pl.Unlock(path)
        err = pl.Lock(path, 10 * time.Millisecond, true)
        if err == nil || !strings.Contains(err.Error(), "timed out") {
            t.Errorf("should have failed to acquire an exclusive lock")
        }

        pl.Unlock(path)
    })

    t.Run("retry", func(t *testing.T) {
        pl := newPathLocks()
        err = pl.Lock(path, 10 * time.Second, true)
        if err != nil {
            t.Fatalf("failed to acquire the lock; %v", err)
        }

        var wait_err error
        var waiter sync.WaitGroup
        waiter.Add(1)
        go func() {
            wait_err = pl.Lock(path, time.Second, true)
            waiter.Done()
        }()

        time.Sleep(500 * time.Millisecond)
        pl.Unlock(path)

        waiter.Wait()
        if wait_err != nil {
            t.Error("second lock should have retried until first was released")
        }
    })
}
