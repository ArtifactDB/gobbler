package main

import (
    "os"
    "fmt"
    "errors"
    "syscall"
    "os/user"
    "strconv"
)

// Just providing uploaders for consistency with gypsum's format
// for the time being, but this should never actually be used.
type Uploader struct {
    Id *string `json:id`
    Asset *string `json:asset`
    Version *string `json:version`
    Until *string `json:until`
    Trusted *string `json:trusted`
}

type Permissions struct {
    Owners []string `json:owners`
    Uploaders []Uploader `json:uploaders`
}

func IdentifyUser(path string) (string, error) {
    sinfo, err := os.Stat(path)
    if err != nil {
        return "", fmt.Errorf("failed to inspect '" + path + "'; %w", err)
    }

    stat, ok := sinfo.Sys().(*syscall.Stat_t)
    if !ok {
        return "", errors.New("failed to determine author of '" + path + "'")
    }

    uinfo, err := user.LookupId(strconv.Itoa(int(stat.Uid)))
    if !ok {
        return "", fmt.Errorf("failed to find user name for author of '" + path + "'; %w", err)
    }
    return uinfo.Username, nil
}
