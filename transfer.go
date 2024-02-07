package main

import (
    "path/filepath"
    "fmt"
    "os"
    "io"
)

func Transfer(source, destination string) error {
    return filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return fmt.Errorf("failed to walk into '" + path + "'; %w", err)
        }
        if path == source {
            return nil
        }

        rel, _ := filepath.Rel(source, path)
        final := filepath.Join(destination, rel)
        if info.IsDir() {
            err := os.MkdirAll(final, 0755)
            if err != nil {
                return fmt.Errorf("failed to create a destination directory at '" + rel + "'; %w", err)
            }
            return nil
        } 

        // Slightly ridiculous to just copy a damn file.
        func() {
            in, err_ := os.Open(path)
            if err_ != nil {
                err = fmt.Errorf("failed to open input file at '" + path + "'; %w", err_)
                return
            }
            defer in.Close()

            out, err_ := os.OpenFile(final, os.O_CREATE, 0644)
            if err_ != nil {
                err = fmt.Errorf("failed to open output file at '" + final + "'; %w", err_)
                return
            }
            defer func() {
                err_ = out.Close()
                if err_ == nil {
                    err = fmt.Errorf("failed to close output file at '" + final + "'; %w", err_)
                }
            }()

            _, err_ = io.Copy(in, out)
            if err_ != nil {
                err = fmt.Errorf("failed to copy '" + rel + "' to the destination; %w", err_)
                return
            }
        }()
        if err != nil {
            return err
        }

        // Double-checking that the source and destination file sizes are equal.
        pinfo, err := os.Stat(path)
        if err != nil {
            return fmt.Errorf("failed to inspect '" + path + "'; %w", err)
        }

        finfo, err := os.Stat(final)
        if err != nil {
            return fmt.Errorf("failed to inspect '" + final + "'; %w", err)
        }

        if pinfo.Size() != finfo.Size() {
            return fmt.Errorf("did not fully copy the contents of '" + rel + "' to the destination")
        }

        return nil
    })
}
