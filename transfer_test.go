package main

import (
    "testing"
    "os"
    "path/filepath"
    "io/ioutil"
    "fmt"
)

func setup_source_and_registry(project, asset, version string) (string, string, error) {
    reg, err := ioutil.TempDir("", "")
    if err != nil {
        return "", "", fmt.Errorf("failed to create the registry; %w", err)
    }

    dir, err := ioutil.TempDir("", "")
    if err != nil {
        return "", "", fmt.Errorf("failed to create the temporary directory; %w", err)
    }

    target := filepath.Join(reg, project, asset, version)
    err = os.MkdirAll(target, 0755)
    if err != nil {
        return "", "", fmt.Errorf("failed to create the version directory; %w", err)
    }

    return reg, dir, nil
}

func mock_source(src string) error {
    err := os.WriteFile(filepath.Join(src, "type"), []byte("electric"), 0644)
    if err != nil {
        return err
    }

    err = os.Mkdir(filepath.Join(src, "evolution"), 0755)
    if err != nil {
        return err
    }
    err = os.WriteFile(filepath.Join(src, "evolution", "up"), []byte("raichu"), 0644)
    if err != nil {
        return err
    }
    err = os.WriteFile(filepath.Join(src, "evolution", "down"), []byte("pichu"), 0644)
    if err != nil {
        return err
    }

    err = os.Mkdir(filepath.Join(src, "moves"), 0755)
    if err != nil {
        return err
    }
    err = os.Mkdir(filepath.Join(src, "moves", "electric"), 0755)
    if err != nil {
        return err
    }
    err = os.WriteFile(filepath.Join(src, "moves", "electric", "thunder_shock"), []byte("40"), 0644)
    if err != nil {
        return err
    }
    err = os.WriteFile(filepath.Join(src, "moves", "electric", "thunderbolt"), []byte("90"), 0644)
    if err != nil {
        return err
    }
    err = os.WriteFile(filepath.Join(src, "moves", "electric", "thunder"), []byte("110"), 0644)
    if err != nil {
        return err
    }
    err = os.Mkdir(filepath.Join(src, "moves", "normal"), 0755)
    if err != nil {
        return err
    }
    err = os.WriteFile(filepath.Join(src, "moves", "normal", "quick_attack"), []byte("40"), 0644)
    if err != nil {
        return err
    }
    err = os.WriteFile(filepath.Join(src, "moves", "normal", "double_team"), []byte("0"), 0644)
    if err != nil {
        return err
    }

    return nil
}

func TestTransferSimple(t *testing.T) {
    project := "pokemon"
    asset := "pikachu"
    version := "red"

    reg, src, err := setup_source_and_registry(project, asset, version)
    if err != nil {
        t.Fatalf("failed to set up test directories; %v", err)
    }

    // Mocking up a directory structure.
    err = mock_source(src)
    if err != nil {
        t.Fatalf("failed to mock up source contents; %v", err)
    }

    // Executing the transfer.
    err = Transfer(src, reg, project, asset, version)
    if err != nil {
        t.Fatalf("failed to perform the transfer; %v", err)
    }

    // Checking a few manifest entries...
    destination := filepath.Join(reg, project, asset, version)
    man, err := ReadManifest(destination)
    if err != nil {
        t.Fatalf("failed to read the manifest; %v", err)
    }
    info, ok := man["evolution/up"]
    if !ok || int(info.Size) != len("raichu") || info.Link != nil {
        t.Fatalf("unexpected manifest entry for 'evolution/up'")
    }
    info, ok = man["moves/electric/thunder"]
    if !ok || int(info.Size) != len("110") || info.Link != nil {
        t.Fatalf("unexpected manifest entry for 'moves/electric/thunder'")
    }

    // Checking some of the actual files.
    payload, err := os.ReadFile(filepath.Join(destination, "type"))
    if err != nil || string(payload) != "electric" {
        t.Fatalf("unexpected contents for 'type'")
    }
    payload, err = os.ReadFile(filepath.Join(destination, "evolution", "down"))
    if err != nil || string(payload) != "pichu" {
        t.Fatalf("unexpected contents for 'evolution/down'")
    }
    payload, err = os.ReadFile(filepath.Join(destination, "moves", "normal", "double_team"))
    if err != nil || string(payload) != "0" {
        t.Fatalf("unexpected contents for 'moves/normal/double_team")
    }
}
