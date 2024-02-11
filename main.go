package main

import (
    "log"
    "github.com/fsnotify/fsnotify"
    "flag"
    "path/filepath"
    "time"
    "os"
    "errors"
    "strings"
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

                // It is expected that request bodies should be initially
                // written to some other file (e.g., `.tmpXXXX`) inside the
                // staging directory, and then moved to the actual file name
                // (`request-<action>-YYY`). The rename should be atomic and
                // thus we avoid problems with the code below triggering before
                // the requester has completed the write of the body. Under
                // this logic, we only have to watch the Create events as
                // no Writes are being performed on a renamed file.
                if event.Has(fsnotify.Create) {
                    info, err := os.Stat(event.Name)
                    if err != nil {
                        log.Println("failed to stat;", err)
                        return
                    }

                    if info.IsDir() {
                        return
                    }

                    basename := filepath.Base(event.Name)
                    if strings.HasPrefix(basename, "request-") {
                        reqtype := strings.TrimPrefix(basename, "request-")

                        if strings.HasPrefix(reqtype, "upload-") {
                            go func(reqpath, basename string) {
                                logpath := filepath.Join(logdir, basename)
                                config, err := Upload(reqpath, registry)
                                if err != nil {
                                    log.Println(err.Error())
                                    err = DumpFailureLog(logpath, err)
                                    if err != nil {
                                        log.Println("failed to dump failure log for '" + basename + "'; ", err)
                                    }
                                } else {
                                    err = DumpSuccessLog(logpath, config.Project, config.Version)
                                    if err != nil {
                                        log.Println("failed to dump success log for '" + basename + "'; ", err)
                                    }
                                }
                            }(event.Name, basename)
                        }
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
