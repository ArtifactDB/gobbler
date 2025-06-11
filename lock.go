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

/* An exclusive lock allows the function to read, delete, create, and modify files or subdirectories or their children within 'dir'.
 *
 * A shared lock guarantees that the contents of 'dir' will not be altered, i.e., no modified files, no new/deleted files or subdirectories.
 * The guarantee only applies to the immediate children of 'dir' and is not recursive.
 *
 * To modify the contents of a directory 'a/b/c', a shared lock should be acquired in each of 'a' and 'b', and an exclusive lock should be acquired on 'c'.
 * Alternatively, we could acquire an exclusive lock on 'b', in which case no lock on 'c' is necessary;
 * or an exclusive lock on 'a', in which case the locks on 'b' and 'c' are also unnecessary.
 * The latter are more powerful but limit parallelism with other functions.
 *
 * Only one "lineage" of locks should be acquired by a function at any given time,
 * i.e., all locks held by that process should apply to directories that are children/parents of other locked directories.
 * Moreover, a lock should be acquired on each parent directory before attempting to acquire a lock on a subdirectory.
 * This ensures that multiple processes are only ever contending for a single lock at their "lowest common ancestor", to avoid deadlocks.
 *
 * The above rules imply that, when promoting a lock from shared to exclusive, all locks on subdirectories should be released.
 */

type directoryLock {
    Globals *globalConfiguration
    Dir string
    Exclusive bool
    Active bool
}

func lockDirectoryExclusive(globals *globalConfiguration, dir string) (*directoryLock, error) {
    err := globals.Locks.Lock(dir, 10 * time.Second, true)
    if err != nil {
        return nil, err
    }
    return &directoryLock{ Globals: globals, Dir: dir, Exclusive: true, Active: true }
}

func lockDirectoryShared(globals *globalConfiguration, dir string) (*directoryLock, error) {
    err := globals.Locks.Lock(dir, 10 * time.Second, false)
    if err != nil {
        return nil, err
    }
    return &directoryLock{ Globals: globals, Dir: dir, Exclusive: false, Active: true }
}

func (dlock *directoryLock) Unlock() {
    if dlock.Active {
        dlock.Globals.Locks.Unlock(dir)
    }
}
