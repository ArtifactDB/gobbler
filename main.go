package main

import (
    "log"
    "github.com/fsnotify/fsnotify"
    "flag"
    "path/filepath"
    "time"
    "os"
    "errors"
)

func main() {
    spath := flag.String("staging", "", "Path to the staging directory to be watched")
    rpath := flag.String("registry", "", "Path to the registry")
    flag.Parse()
    if *spath == "" || *rpath == "" {
        flag.Usage()
        os.Exit(1)
    }
    staging := *spath
    registry := *rpath

    logdir := filepath.Join(staging, "..logs")
    if _, err := os.Stat(logdir); errors.Is(err, os.ErrNotExist) {
        err := os.Mkdir(logdir, 0755)
        if err != nil {
            log.Fatal("failed to create a log subdirectory; ", err)
        }
    }

    // Launching a watcher to pick up changes and launch jobs.
    watcher, err := fsnotify.NewWatcher()
    if err != nil {
        log.Fatal("failed to start the watcher on the staging directory; ", err)
    }
    defer watcher.Close()

    go func() {
        for {
            select {
            case event, ok := <-watcher.Events:
                if !ok {
                    return
                }
                log.Println("triggered filesystem event:", event)

                if event.Has(fsnotify.Create) {
                    info, err := os.Stat(event.Name)
                    if err != nil {
                        log.Println("failed to stat;", err)
                        return
                    }

                    if info.IsDir() {
                        log.Println("added directory to the watch list")
                        watcher.Add(event.Name)
                        return
                    }

                    if filepath.Base(event.Name) == "_DONE" {
                        subdir := filepath.Dir(event.Name)

                        // Protect against jokers who just put a DONE in the top-level directory.
                        basename := filepath.Base(subdir)
                        if basename == staging { 
                            return
                        }

                        go func(subdir, basename string) {
                            logpath := filepath.Join(logdir, basename)

                            var fail_err error
                            fail_err = nil
                            defer func() {
                                err := DumpFailureLog(logpath, fail_err)
                                if err != nil {
                                    log.Println("failed to dump failure log for '" + basename + "'; ", err)
                                }
                            }()

                            config, err := Configure(subdir, registry)
                            if err != nil {
                                log.Println("failed to choose destination for '" + basename + "'; ", err)
                                fail_err = err
                                return
                            }

                            destdir := filepath.Join(registry, config.Project, config.Version)
                            err = Transfer(subdir, destdir)
                            if err != nil {
                                log.Println("failed to transfer files to the destination for '" + basename + "'; ", err)
                                fail_err = err
                                return
                            }

                            err = DumpVersionMetadata(filepath.Join(destdir, "..metadata"), config.User)
                            if err != nil {
                                log.Println("failed to dump version metadata for '" + basename + "'; ", err)
                                fail_err = err
                                return
                            }

                            err = DumpSuccessLog(logpath, config.Project, config.Version)
                            if err != nil {
                                log.Println("failed to dump success log for '" + basename + "'; ", err)
                                return
                            }

                            err = os.RemoveAll(subdir)
                            if err != nil {
                                log.Println("failed to delete '" + basename + "'; ", err)
                            }
                        }(subdir, basename)
                    }

                } else if event.Has(fsnotify.Remove) {
                    err := watcher.Remove(event.Name)
                    if err == nil {
                        log.Println("removed directory from the watch list")
                    }
                }

            case err, ok := <-watcher.Errors:
                if !ok {
                    return
                }
                log.Println("watcher error;", err)
            }
        }
    }()

    err = watcher.Add(staging)
    if err != nil {
        log.Fatal(err)
    }

    // Adding a per-day job that purges old files.
	ticker := time.NewTicker(time.Hour * 24)
	defer ticker.Stop()
    go func() {
        for {
            <-ticker.C
            err := PurgeOldFiles(staging, time.Hour * 24 * 7)
            if err != nil {
                log.Println(err)
            }
        }
    }()

    // Block main goroutine forever.
    <-make(chan struct{})
}
