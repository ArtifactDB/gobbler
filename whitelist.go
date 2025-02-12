package main

import (
    "strings"
    "os"
    "fmt"
    "bufio"
)

func isLinkWhitelisted(path string, whitelist []string) bool {
    for _, w := range whitelist {
        if strings.HasPrefix(path, w) {
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
        output = append(output, scanner.Text())
    }

    if err := scanner.Err(); err != nil {
        return nil, fmt.Errorf("failed to parse the whitelist file; %v", err)
    }
    return output, nil
}
