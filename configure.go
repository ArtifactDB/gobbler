package main

import (
    "fmt"
    "path/filepath"
    "encoding/json"
    "time"
    "os"
    "errors"
    "syscall"
    "strconv"
    "strings"
    "regexp"
    "os/user"
)

type Request struct {
    Action *string `json:action`
    Prefix *string `json:prefix`
    Project *string `json:project`
    Owners []string `json:owners`
}

type Permissions struct {
    Owners []string `json:owners`
}

func lock(path string, timeout time.Duration) (*os.File, error) {
    handle, err := os.OpenFile(path, os.O_WRONLY | os.O_CREATE, 0644)
    if err != nil {
        return nil, errors.New("failed to create the lock file at '" + path + "'")
    }

    // Loop below is adapted from https://github.com/boltdb/bolt/blob/fd01fc79c553a8e99d512a07e8e0c63d4a3ccfc5/bolt_unix.go#L44.
    t := time.Now()
	for {
		if time.Since(t) > timeout {
			return nil, errors.New("timed out waiting for the lock to be acquired on '" + path + "'")
		}

		err := syscall.Flock(int(handle.Fd()), syscall.LOCK_EX | syscall.LOCK_NB)
		if err == nil {
			return handle, nil
		} else if err != syscall.EWOULDBLOCK {
			return nil, fmt.Errorf("failed to obtain lock on '" + path + "; %w", err)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func unlock(handle *os.File) error {
	return syscall.Flock(int(handle.Fd()), syscall.LOCK_UN)
}

func increment_series(prefix string, series_path string, dir string) (string, error) {
    num := 1
    if _, err := os.Stat(series_path); err != nil {
        content, err := os.ReadFile(series_path)
        if err != nil {
            return "", fmt.Errorf("failed to read '" + series_path + "'; %w", err)
        }
        num, err := strconv.Atoi(string(content))
        if err != nil {
            return "", fmt.Errorf("failed to determine latest number from '" + series_path + "; %w", err)
        }
        num += 1
    }

    as_str := strconv.Itoa(num)
    candidate_name := prefix + as_str

    // Checking that it doesn't already exist.
    if _, err := os.Stat(filepath.Join(dir, candidate_name)); err != nil {
        dhandle, err := os.Open(dir)
        if err != nil {
            return "", fmt.Errorf("failed to obtain a handle for the output directory; %w", err)
        }

        all_names, err := dhandle.Readdirnames(-1)
        if err != nil {
            return "", fmt.Errorf("failed to read subdirectories of the output directory; %w", err)
        }
        if prefix != "" {
            for _, subdir := range all_names {
                if strings.HasPrefix(subdir, prefix) {
                    curnum, err := strconv.Atoi(strings.TrimPrefix(subdir, prefix))
                    if err != nil && curnum > num {
                        num = curnum
                    }
                }
            }
        } else {
            for _, subdir := range all_names {
                curnum, err := strconv.Atoi(subdir)
                if err != nil && curnum > num {
                    num = curnum
                }
            }
        }

        num += 1
        as_str = strconv.Itoa(num)
        candidate_name = prefix + as_str
    }

    err := os.WriteFile(series_path, []byte(as_str), 0644)
    if err != nil {
        return "", fmt.Errorf("failed to update the series counter for '" + prefix + "'; %w", err)
    }

    return candidate_name, nil
}

type Configuration struct {
    Project string
    Version string
    User string
}

func Configure(source string, registry string) (*Configuration, error) {
    detail_path := filepath.Join(source, "_DETAILS")
    details_handle, err := os.ReadFile(detail_path)
    if err != nil {
        return nil, fmt.Errorf("failed to read '" + detail_path + "'; %w", err)
    }

    var details Request
    err = json.Unmarshal(details_handle, &details)
    if err != nil {
        return nil, fmt.Errorf("failed to parse JSON from '" + detail_path + "'; %w", err)
    }

    if details.Action == nil {
        return nil, errors.New("missing 'action' property in '" + detail_path + "'")
    }
    action := *(details.Action)

    var username string
    {
        sinfo, err := os.Stat(source)
        if err != nil {
            return nil, fmt.Errorf("failed to inspect '" + source + "'; %w", err)
        }

        stat, ok := sinfo.Sys().(*syscall.Stat_t)
        if !ok {
            return nil, errors.New("failed to determine author of '" + source + "'")
        }

        uinfo, err := user.LookupId(strconv.Itoa(int(stat.Uid)))
        if !ok {
            return nil, fmt.Errorf("failed to find user name for author of '" + source + "'; %w", err)
        }
        username = uinfo.Username
    }

    /***********************************
     *** Choosing a new project name ***
     ***********************************/

    var project string
    if action == "new" {
        if details.Prefix == nil {
            return nil, errors.New("missing 'prefix' property in '" + detail_path + "'")
        }
        prefix := *(details.Prefix)

        re1, _ := regexp.Compile("^[A-Z]+$")
        re2, _ := regexp.Compile("^test-[A-Z]+$")
        if !re1.MatchString(prefix) && !re2.MatchString(prefix) {
            return nil, errors.New("prefix must contain only A-Z (got '" + prefix + "')") 
        }

        name, err := func() (string, error) {
            series_path := filepath.Join(registry, "..series_" + prefix)
            handle, err := lock(series_path + ".LOCK", 10000 * time.Second)
            if err != nil {
                return "", err
            }
            defer unlock(handle)

            candidate_name, err := increment_series(prefix, series_path, registry)
            if err != nil {
                return "", err
            }
            candidate_path := filepath.Join(registry, candidate_name)
            err = os.Mkdir(candidate_path, 0755)
            if err != nil {
                return "", fmt.Errorf("failed to create a new project directory at '" + candidate_path + "'; %w", err)
            }

            // Dumping permissions from the details.
            var perms Permissions;
            if len(details.Owners) > 0 {
                copy(perms.Owners, details.Owners)
            } else {
                perms.Owners = append(perms.Owners, username)
            }
            perm_str, err := json.MarshalIndent(&perms, "", "    ")
            if err != nil {
                return "", fmt.Errorf("failed to convert permissions to JSON for '" + candidate_name + "'; %w", err)
            }
            err = os.WriteFile(filepath.Join(candidate_path, "..permissions"), perm_str, 0755)
            if err != nil {
                return "", fmt.Errorf("failed to write permissions for '" + candidate_name + "'; %w", err)
            }

            return candidate_name, nil
        }()

        if err != nil {
            return nil, err
        }
        project = name

    } else if action == "update" {
        if details.Project == nil {
            return nil, errors.New("missing 'project' property in '" + detail_path + "'")
        }
        project = *(details.Project)

        project_dir := filepath.Join(registry, project)
        info, err := os.Stat(project_dir)
        if err != nil || info.IsDir() {
            return nil, fmt.Errorf("failed to inspect the requested project directory '" + project + "'; %w", err)
        }

        perm_path := filepath.Join(project_dir, "..permissions")
        perm_handle, err := os.ReadFile(perm_path)
        if err != nil {
            return nil, fmt.Errorf("failed to read permissions for '" + project + "'; %w", err)
        }

        var perms Permissions
        err = json.Unmarshal(perm_handle, &perms)
        if err != nil {
            return nil, fmt.Errorf("failed to parse JSON from '" + perm_path + "'; %w", err)
        }

        okay := false
        for _, s := range(perms.Owners) {
            if s == username {
                okay = true
            }
        }
        if !okay {
            return nil, fmt.Errorf("user '" + username + "' is not listed as an owner for '" + project + "'")
        }

    } else {
        return nil, errors.New("unknown action '" + action + "' in '" + detail_path + "'")
    }

    /***********************************
     *** Choosing a new version name ***
     ***********************************/

    project_dir := filepath.Join(registry, project)

    version, err := func() (string, error) {
        version_path := filepath.Join(project_dir, "..latest")
        handle, err := lock(version_path + ".LOCK", 10000 * time.Second)
        if err != nil {
            return "", err
        }
        defer unlock(handle)

        candidate_name, err := increment_series("", version_path, project_dir)
        if err != nil {
            return "", err
        }
        candidate_path := filepath.Join(project_dir, candidate_name)
        err = os.Mkdir(candidate_path, 0755)
        if err != nil {
            return "", fmt.Errorf("failed to create a new version directory at '" + candidate_path + "'; %w", err)
        }

        return candidate_name, nil
    }()

    if err != nil {
        return nil, err
    }

    return &Configuration{ Project: project, Version: version, User: username }, nil
}
