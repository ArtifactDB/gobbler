package main

import (
    "time"
    "fmt"
    "sync"
)

type pathLocks struct {
    Lock sync.Mutex 
    InUse map[string]bool
}

func newPathLocks() pathLocks {
    return pathLocks{ InUse: map[string]bool{} }
}

func (pl *pathLocks) LockPath(path string, timeout time.Duration) error {
    var t time.Time
    init := true
    for {
        if init {
            t = time.Now()
            init = false
        } else if time.Since(t) > timeout {
            return fmt.Errorf("timed out waiting for the lock to be acquired on %q", path)
        }

        already_locked := func() bool {
            pl.Lock.Lock()
            defer pl.Lock.Unlock()

            _, ok := pl.InUse[path]
            if !ok {
                pl.InUse[path] = true
                return false
            } else {
                return true
            }
        }()

        if !already_locked {
            return nil
        }

        time.Sleep(time.Millisecond * 50)
    }
}

func (pl* pathLocks) UnlockPath(path string) {
    pl.Lock.Lock()
    defer pl.Lock.Unlock()
    delete(pl.InUse, path)
}
