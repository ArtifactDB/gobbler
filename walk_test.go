package main

import (
    "os"
    "fmt"
    "path/filepath"
)

func setupSourceForWalkDirectoryTest() (string, error) {
    dir, err := os.MkdirTemp("", "")
    if err != nil {
        return "", fmt.Errorf("failed to create the temporary directory; %w", err)
    }

    err = os.WriteFile(filepath.Join(dir, "type"), []byte("electric"), 0644)
    if err != nil {
        return "", err
    }

    err = os.Mkdir(filepath.Join(dir, "evolution"), 0755)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(dir, "evolution", "up"), []byte("raichu"), 0644)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(dir, "evolution", "down"), []byte("pichu"), 0644)
    if err != nil {
        return "", err
    }

    err = os.Mkdir(filepath.Join(dir, "moves"), 0755)
    if err != nil {
        return "", err
    }
    err = os.Mkdir(filepath.Join(dir, "moves", "electric"), 0755)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(dir, "moves", "electric", "thunder_shock"), []byte("40"), 0644)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(dir, "moves", "electric", "thunderbolt"), []byte("90"), 0644)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(dir, "moves", "electric", "thunder"), []byte("110"), 0644)
    if err != nil {
        return "", err
    }

    err = os.Mkdir(filepath.Join(dir, "moves", "normal"), 0755)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(dir, "moves", "normal", "quick_attack"), []byte("40"), 0644)
    if err != nil {
        return "", err
    }
    err = os.WriteFile(filepath.Join(dir, "moves", "normal", "double_team"), []byte("0"), 0644)
    if err != nil {
        return "", err
    }

    return dir, nil
}
