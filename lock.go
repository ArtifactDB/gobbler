package main

import (
    "time"
    "fmt"
    "sync"
    "os"
    "syscall"
    "path/filepath"
    "context"
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

func (pl *pathLocks) Lock(path string, ctx context.Context, timeout time.Duration, exclusive bool) error {
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

        err := ctx.Err()
        if err != nil {
            return fmt.Errorf("lock request cancelled; %w", err)
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

/* An exclusive directory lock allows the function to read, delete, create, and modify files or subdirectories or their children within 'dir'.
 *
 * A shared directory lock guarantees that the contents of 'dir' will not be modified or deleted (with the exception of the project usage file) and no new files will be added.
 * The guarantee only applies to the immediate children of 'dir' and is not recursive.
 *
 * An exclusive new-dir directory lock allows the function to create new subdirectories in 'dir'.
 * The privilege only applies to the immediate children of 'dir' and is not recursive.
 *
 * A shared new-dir directory lock gurantees that no new subdirectories will be added to 'dir'.
 * The guarantee only applies to the immediate children of 'dir' and is not recursive.
 *
 * To enter an existing subdirectory, it is necessary to acquire a shared lock on the directory.
 * Then, the function must acquire a new-dir lock (exclusive or shared) to protect against race conditions with directory creation.
 * Once this is done, the function can check whether or not the subdiirectory exists.
 * Finally, the new-dir lock can be released to allow other functions to perform their checks.
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
 * Acquiring a new-dir lock should only be performed once a shared lock on the associated directory is acquired.
 * Moreover, it must be acquired before acquiring any locks on subdirectories, so as to avoid deadlocks.
 * A new-dir lock can be released at any time before the shared lock on its directory is released.
 *
 * Consider a common situation where we acquire an exclusive new-dir lock, check that a directory does not exist, and create a new directory.
 * This effectively gives us an exclusive lock on the newly created directory while we hold that new-dir lock.
 * No other function can acquire a shared new-dir lock to check that the directory exists before entering it;
 * and obviously, the directory would not have existed before we acquired the lock, so no function could have already entered it and released the new-dir lock.
 */

type directoryLock struct {
    LockFile string
    Active bool
}

func lockDirectoryExclusive(dir string, globals *globalConfiguration, ctx context.Context) (*directoryLock, error) {
    lockfile := filepath.Join(dir, "..LOCK")
    err := globals.Locks.Lock(lockfile, ctx, 60 * time.Second, true)
    if err != nil {
        return nil, err
    }
    return &directoryLock{ LockFile: lockfile, Active: true }, nil
}

func lockDirectoryShared(dir string, globals *globalConfiguration, ctx context.Context) (*directoryLock, error) {
    lockfile := filepath.Join(dir, "..LOCK")
    err := globals.Locks.Lock(lockfile, ctx, 60 * time.Second, false)
    if err != nil {
        return nil, err
    }
    return &directoryLock{ LockFile: lockfile, Active: true }, nil
}

func (dlock *directoryLock) Unlock(globals *globalConfiguration) {
    if dlock.Active {
        globals.Locks.Unlock(dlock.LockFile)
        dlock.Active = false
    }
}

func lockDirectoryNewDirShared(dir string, globals *globalConfiguration, ctx context.Context) (*directoryLock, error) {
    lockfile := filepath.Join(dir, "..LOCK_NEWDIR")
    err := globals.Locks.Lock(lockfile, ctx, 60 * time.Second, false)
    if err != nil {
        return nil, err
    }
    return &directoryLock{ LockFile: lockfile, Active: true }, nil
}

func lockDirectoryNewDirExclusive(dir string, globals *globalConfiguration, ctx context.Context) (*directoryLock, error) {
    lockfile := filepath.Join(dir, "..LOCK_NEWDIR")
    err := globals.Locks.Lock(lockfile, ctx, 60 * time.Second, true)
    if err != nil {
        return nil, err
    }
    return &directoryLock{ LockFile: lockfile, Active: true }, nil
}

/* The usage lock is a special beast: it allows the function to read and write the project usage file.
 * It should only be acquired after a shared or exclusive lock is acquired on the project directory, and released before that project directory lock is released.
 * To avoid deadlocks, no attempt should be made to acquire another lock (e.g., the asset directory lock) while holding a usage lock.
 * However, locks that have already been acquired are fine.
 *
 * The concept of the usage lock is based on the expectation that the order of deltas to the project usage is not important.
 * All of uploadHandler(), deleteAssetHandler(), deleteProjectHandler() and rejectProbationHandler() will contribute edits to the usage.
 * Contention for the usage lock means that the edits may be performed in a different order to the actual modifications on the filesystem,
 * e.g., the usage may be momentarily negative if a newly uploaded version was deleted and the deletion's reduction in usage was processed before the upload's addition.
 * This is acceptable as long as all edits are eventually processed to give the correct value. 
 *
 * The usage lock does not need to be acquired while holding the exclusive lock that was used to protect the filesystem modification.
 * However, it assumes that the calculation of the usage delta is performed under the same exclusive lock as the modification.
 * This ensures that the correct delta is applied to the project usage, even after an arbitrarily long period of contention for the usage lock.
 * Otherwise, if the delta is computed outside of the exclusive lock, some other process could have altered the filesystem once the exclusive lock is released.
 */
func lockDirectoryWriteUsage(dir string, globals *globalConfiguration, ctx context.Context) (*directoryLock, error) {
    lockfile := filepath.Join(dir, "..LOCK_USAGE")
    err := globals.Locks.Lock(lockfile, ctx, 60 * time.Second, true)
    if err != nil {
        return nil, err
    }
    return &directoryLock{ LockFile: lockfile, Active: true }, nil
}
