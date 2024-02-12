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
    mstr := flag.String("admin", "", "Comma-separated list of administrators.")
    flag.Parse()

    if *spath == "" || *rpath == "" {
        flag.Usage()
        os.Exit(1)
    }
    staging := *spath
    registry := *rpath

    administrators := []string{}
    if *mstr != "" {
        administrators = strings.Split(*mstr, ",")
    }

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

                        go func(reqpath, basename string) {
                            var reportable_err error
                            payload := map[string]string{}

                            if strings.HasPrefix(reqtype, "upload-") {
                                config, err0 := uploadHandler(reqpath, registry, administrators)
                                if err0 != nil {
                                    payload["project"] = config.Project
                                    payload["version"] = config.Version
                                } else {
                                    reportable_err = err0
                                }
                            } else if strings.HasPrefix(reqtype, "set_permissions-") {
                                reportable_err = setPermissionsHandler(reqpath, registry, administrators)
                            } else if strings.HasPrefix(reqtype, "refresh_latest-") {
                                reportable_err = refreshLatestHandler(reqpath, registry, administrators)
                            } else if strings.HasPrefix(reqtype, "refresh_usage-") {
                                reportable_err = refreshUsageHandler(reqpath, registry, administrators)
                            }

                            if reportable_err == nil {
                                payload["status"] = "SUCCESS"
                            } else {
                                log.Println(reportable_err.Error())
                                payload = map[string]string{
                                    "status": "FAILED",
                                    "reason": reportable_err.Error(),
                                }
                            }

                            logpath := filepath.Join(logdir, basename)
                            err = dumpJson(logpath, payload)
                            if err != nil {
                                log.Println("failed to dump response for '" + basename + "'; ", err)
                            }
                        }(event.Name, basename)
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
            err := purgeOldFiles(staging, time.Hour * 24 * 7)
            if err != nil {
                log.Println(err)
            }
        }
    }()

    // Block main goroutine forever.
    <-make(chan struct{})
}
