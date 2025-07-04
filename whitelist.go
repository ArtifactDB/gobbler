package main

import (
    "os"
    "fmt"
    "bufio"
    "path/filepath"
)

func isLinkWhitelisted(path string, whitelist []string) bool {
    for _, w := range whitelist {
        rel, err := filepath.Rel(w, path)
        if err == nil && filepath.IsLocal(rel) {
            return true
        }
    }
    return false
}

func loadLinkWhitelist(path string) ([]string, error) {
    whandle, err := os.Open(path)
    if err != nil {
        return nil, fmt.Errorf("failed to open the whitelist file; %v", err)
    }
    defer whandle.Close()

    output := []string{}
    scanner := bufio.NewScanner(whandle)
    for scanner.Scan() {
        path := filepath.Clean(scanner.Text())
        if !filepath.IsAbs(path) {
            return nil, fmt.Errorf("expected the whitelist file to contain absolute paths, got %v", path)
        }
        output = append(output, path)
    }

    if err := scanner.Err(); err != nil {
        return nil, fmt.Errorf("failed to parse the whitelist file; %v", err)
    }
    return output, nil
}
