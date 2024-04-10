package main

import (
    "time"
    "fmt"
    "sync"
    "os"
    "syscall"
    "path/filepath"
)

type pathLocks struct {
    Lock sync.Mutex 
    InUse map[string]*os.File
}

func newPathLocks() pathLocks {
    return pathLocks{ InUse: map[string]*os.File{} }
}

func (pl *pathLocks) obtainLock(path string, lockfile string, timeout time.Duration) error {
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
            if ok {
                return true
            }

            // Place an advisory lock across multiple gobbler processes. 
            file, err := os.OpenFile(lockfile, os.O_RDWR|os.O_CREATE, 0666)
            if err != nil { // Maybe we failed to write it because the handle was opened by some other process.
                return true
            }

            err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
            if err != nil { // The lock failed because of contention, or permissions, or who knows.
                file.Close()
                return true
            }

            pl.InUse[path] = file
            return false
        }()

        if !already_locked {
            return nil
        }

        time.Sleep(time.Millisecond * 50)
    }
}

func (pl *pathLocks) LockDirectory(path string, timeout time.Duration) error {
    return pl.obtainLock(path, filepath.Join(path, "..LOCK"), timeout)
}

func (pl* pathLocks) Unlock(path string) {
    pl.Lock.Lock()
    defer pl.Lock.Unlock()

    file := pl.InUse[path]
    defer file.Close()

    syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
    delete(pl.InUse, path)
}
