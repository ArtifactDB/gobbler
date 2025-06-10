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
            file, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE, 0666)
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

/* A strong lock allows the process to read, delete, create, and modify files or subdirectories or their children within 'dir'.
 *
 * A weak lock only guarantees that any existing subdirectories in 'dir' will not be deleted. 
 * The guarantee only applies to the immediate children of 'dir' and is not recursive.
 *
 * A promoted weak lock allows the process to read, create and modify files or subdirectories within 'dir'.
 * These permissions are not recursive.
 *
 * - If we want to go to a subdirectory, the process is a bit involved.
 *   First, we acquire a promoted lock on the directory, check if the subdirectory exists, and possibly create it if it doesn't.
 *   Then, we demote the promoted lock to a weak lock to free up other processes, and proceed to the subdirectory.
 * - If we want to modify subdirectories without acquiring a promoted lock on each one, we can just acquire a strong lock on the parent directory.
 *   This is because only strong locks apply recursively.
 * - If we realize that we wanted to modify a file in a directory, we can promote the directory's weak lock to a promoted lock.
 *   Keep in mind that the promotion may be contended, so a process's modification may occur after other modifications on the same file.
 */

type directoryLock struct {
    Globals *globalConfiguration
    Dir string
    Promoted bool
}

func lockDirectoryWeak(globals *globalConfiguration, dir string) (*directoryLock, error) {
    path := filepath.Join(dir, "..LOCK")
    err := globals.Locks.Lock(path, 10 * time.Second, false)
    if err != nil {
        return nil, err
    }
    return &directoryLock{ Globals: globals, Dir: dir, Promoted: false }, nil
}

func lockDirectoryStrong(globals *globalConfiguration, dir string) (*directoryLock, error) {
    path := filepath.Join(dir, "..LOCK")
    err := globals.Locks.Lock(path, 10 * time.Second, true)
    if err != nil {
        return nil, err
    }
    return &directoryLock{ Globals: globals, Dir: dir, Promoted: false }, nil
}

func lockDirectoryPromoted(globals *globalConfiguration, dir string) (*directoryLock, error) {
    dlock, err := lockDirectoryWeak(globals, dir)
    if err != nil {
        return nil, err
    }
    err = dlock.Promote()
    if err != nil {
        return nil, err
    }
    return dlock, nil
}

func (dlock *directoryLock) Promote() error {
    path := filepath.Join(dlock.Dir, "..LOCK_EXTRA")
    err := dlock.Globals.Locks.Lock(path, 10 * time.Second, true)
    if err != nil {
        return err
    }
    dlock.Promoted = true
    return nil
}

func (dlock *directoryLock) Demote() {
    if !dlock.Promoted {
        return
    }
    path := filepath.Join(dlock.Dir, "..LOCK_EXTRA")
    dlock.Globals.Locks.Unlock(path)
    dlock.Promoted = false
    return
}

func (dlock *directoryLock) Unlock() {
    if dlock.Promoted {
        dlock.Demote()
    }
    path := filepath.Join(dlock.Dir, "..LOCK")
    dlock.Globals.Locks.Unlock(path)
    return
}
