package main

import (
    "time"
    "fmt"
    "sync"
    "os"
    "syscall"
    "path/filepath"
)

type pathLock struct {
    Handle *os.File
    IsShared bool
    NumShared int
}

type pathLocks struct {
    UseLock sync.Mutex 
    InUse map[string]*pathLock
}

func newPathLocks() pathLocks {
    return pathLocks{ InUse: map[string]*pathLock{} }
}

func (pl *pathLocks) Lock(path string, timeout time.Duration, exclusive bool) error {
    lockfile := filepath.Join(path, "..LOCK")
    var lock_mode int
    if exclusive { 
        lock_mode = syscall.LOCK_EX
    } else {
        lock_mode = syscall.LOCK_SH
    }

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
            pl.UseLock.Lock()
            defer pl.UseLock.Unlock()

            val, ok := pl.InUse[path]
            if ok {
                if exclusive {
                    return true
                } else {
                    if !val.IsShared {
                        return true
                    } else {
                        val.NumShared += 1
                        return false
                    }
                }
            }

            // Place an advisory lock across multiple gobbler processes. 
            file, err := os.OpenFile(lockfile, os.O_RDWR | os.O_CREATE, 0666)
            if err != nil { // Maybe we failed to write it because the handle was opened by some other process.
                return true
            }

            err = syscall.Flock(int(file.Fd()), lock_mode | syscall.LOCK_NB)
            if err != nil { // The lock failed because of contention, or permissions, or who knows.
                file.Close()
                return true
            }

            pl.InUse[path] = &pathLock{ Handle: file, IsShared: !exclusive, NumShared: 1 }
            return false
        }()

        if !already_locked {
            return nil
        }

        time.Sleep(time.Millisecond * 50)
    }
}

func (pl* pathLocks) Unlock(path string) {
    pl.UseLock.Lock()
    defer pl.UseLock.Unlock()

    val := pl.InUse[path]
    if val.IsShared {
        if val.NumShared > 1 {
            val.NumShared -= 1
            return
        }
    }

    defer val.Handle.Close()
    syscall.Flock(int(val.Handle.Fd()), syscall.LOCK_UN)
    delete(pl.InUse, path)
}

func lockRegistry(globals *globalConfiguration, timeout time.Duration) error {
    return globals.Locks.Lock(globals.Registry, timeout, true)
}

func unlockRegistry(globals *globalConfiguration) {
    globals.Locks.Unlock(globals.Registry)
}

func lockProject(globals *globalConfiguration, project_dir string, timeout time.Duration) error {
    err := globals.Locks.Lock(globals.Registry, timeout, false)
    if err != nil {
        return err
    }
    err = globals.Locks.Lock(project_dir, timeout, true)
    if err != nil {
        globals.Locks.Unlock(globals.Registry)
    }
    return err
}

func unlockProject(globals *globalConfiguration, project_dir string) {
    globals.Locks.Unlock(project_dir)
    globals.Locks.Unlock(globals.Registry)
}
