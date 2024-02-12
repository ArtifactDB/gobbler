package main

import (
    "os"
    "errors"
    "time"
    "syscall"
    "fmt"
)

const lockFileName = "..LOCK"

func lock(path string, timeout time.Duration) (*os.File, error) {
    handle, err := os.OpenFile(path, os.O_WRONLY | os.O_CREATE, 0644)
    if err != nil {
        return nil, errors.New("failed to create the lock file at '" + path + "'")
    }

    // Loop below is adapted from https://github.com/boltdb/bolt/blob/fd01fc79c553a8e99d512a07e8e0c63d4a3ccfc5/bolt_unix.go#L44.
    t := time.Now()
    init := true
	for {
		if !init && time.Since(t) > timeout {
			return nil, errors.New("timed out waiting for the lock to be acquired on '" + path + "'")
		}

		err := syscall.Flock(int(handle.Fd()), syscall.LOCK_EX | syscall.LOCK_NB)
		if err == nil {
			return handle, nil
		} else if err != syscall.EWOULDBLOCK {
			return nil, fmt.Errorf("failed to obtain lock on '" + path + "; %w", err)
		}

        init = false
		time.Sleep(50 * time.Millisecond)
	}
}

func unlock(handle *os.File) error {
    err := syscall.Flock(int(handle.Fd()), syscall.LOCK_UN)
    if err != nil {
        return fmt.Errorf("failed to unlock '" + handle.Name() + "'; %w", err)
    }
    return nil
}
