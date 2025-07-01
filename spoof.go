package main

import (
    "os"
    "fmt"
    "bufio"
    "strings"
    "net/http"
)

type spoofPermissions struct {
    All bool
    Users map[string]bool
    Exclude map[string]bool
}

func isSpoofingAllowed(spoofer, user string, permissions map[string]spoofPermissions) bool {
    val, ok := permissions[spoofer]
    if !ok {
        return false
    }

    if val.All {
        if _, ok := val.Exclude[user]; ok {
            return false;
        }
        return true
    }

    _, ok = val.Users[user]
    return ok
}

func identifySpoofedUser(path string, spoof *string, allowed map[string]spoofPermissions) (string, error) {
    user, err := identifyUser(path)
    if err != nil {
        return "", fmt.Errorf("failed to find owner of %q; %w", path, err)
    }

    if spoof == nil {
        return user, nil 
    }

    if !isSpoofingAllowed(user, *spoof, allowed) {
        return "", newHttpError(http.StatusForbidden, fmt.Errorf("user %q is not authorized to spoof %q", user, *spoof))
    }

    return *spoof, nil
}

func loadSpoofPermissions(path string) (map[string]spoofPermissions, error) {
    shandle, err := os.Open(path)
    if err != nil {
        return nil, fmt.Errorf("failed to open the spoofing permissions file; %v", err)
    }
    defer shandle.Close()

    output := map[string]spoofPermissions{}
    scanner := bufio.NewScanner(shandle)
    for scanner.Scan() {
        info := scanner.Text()

        separated := strings.Split(info, ":")
        if len(separated) != 2 {
            return nil, fmt.Errorf("unexpected format for line %q in the spoofing permissions file", info);
        }
        if len(separated[1]) == 0 {
            continue
        }

        spoofer := separated[0]
        users := strings.Split(separated[1], ",")

        use_all := false
        user_set := map[string]bool{}
        exclude_set := map[string]bool{}
        for _, u := range users {
            if (u == "*") {
                use_all = true
            } else if strings.HasPrefix(u, "-") {
                exclude_set[u[1:]] = true
            } else {
                user_set[u] = true
            }
        }

        output[spoofer] = spoofPermissions{ All: use_all, Users: user_set, Exclude: exclude_set }
    }

    if err := scanner.Err(); err != nil {
        return nil, fmt.Errorf("failed to parse the spoofing permissions file; %v", err)
    }
    return output, nil
}
