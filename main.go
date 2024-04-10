package main

import (
    "log"
    "flag"
    "path/filepath"
    "time"
    "os"
    "errors"
    "strings"
    "fmt"
    "encoding/json"
    "net/http"
    "strconv"
)

func dumpJsonResponse(w http.ResponseWriter, status int, v interface{}, path string) {
    contents, err := json.Marshal(v)
    if err != nil {
        log.Printf("failed to convert response to JSON for %q; %v", path, err)
        contents = []byte("unknown")
    }

    w.Header().Set("Content-Type", "application/json")
    w.Header().Set("Access-Control-Allow-Origin", "*")
    w.WriteHeader(status)
    _, err = w.Write(contents)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        log.Printf("failed to write JSON response for %q; %v", path, err)
    }
}

func dumpErrorResponse(w http.ResponseWriter, status int, message string, path string) {
    log.Printf("failed to process %q; %s\n", path, message)
    dumpJsonResponse(w, status, map[string]interface{}{ "status": "ERROR", "reason": message }, path)
}

func main() {
    spath := flag.String("staging", "", "Path to the staging directory.")
    rpath := flag.String("registry", "", "Path to the registry.")
    mstr := flag.String("admin", "", "Comma-separated list of administrators.")
    port := flag.Int("port", 8080, "Port to listen to API requests.")
    flag.Parse()

    if *spath == "" || *rpath == "" {
        flag.Usage()
        os.Exit(1)
    }

    staging := *spath
    globals := newGlobalConfiguration(*rpath)
    if *mstr != "" {
        globals.Administrators = strings.Split(*mstr, ",")
    }

    log_dir := filepath.Join(globals.Registry, logDirName)
    if _, err := os.Stat(log_dir); errors.Is(err, os.ErrNotExist) {
        err := os.Mkdir(log_dir, 0755)
        if err != nil {
            log.Fatal("failed to create a log subdirectory; ", err)
        }
    }

    // Launching a watcher to pick up changes and launch jobs.
    http.HandleFunc("/new/{path}", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != "POST" {
            dumpErrorResponse(w, http.StatusMethodNotAllowed, "expected a POST request", "???")
            return 
        }

        path := filepath.Base(r.PathValue("path"))
        log.Println("processing " + path)

        if !strings.HasPrefix(path, "request-") {
            dumpErrorResponse(w, http.StatusBadRequest, "file name should start with \"request-\"", path)
            return 
        }
        reqtype := strings.TrimPrefix(path, "request-")

        reqpath := filepath.Join(staging, path)
        info, err := os.Stat(reqpath)
        if err != nil {
            dumpErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("failed to stat; %v", err), path)
            return 
        }
        if info.IsDir() {
            dumpErrorResponse(w, http.StatusBadRequest, "path is a directory", path)
            return 
        }

        var reportable_err error
        payload := map[string]interface{}{}

        if strings.HasPrefix(reqtype, "upload-") {
            reportable_err = uploadHandler(reqpath, &globals)

        } else if strings.HasPrefix(reqtype, "refresh_latest-") {
            res, err0 := refreshLatestHandler(reqpath, &globals)
            if err0 == nil {
                if res != nil {
                    payload["version"] = res.Version
                }
            } else {
                reportable_err = err0
            }

        } else if strings.HasPrefix(reqtype, "refresh_usage-") {
            res, err0 := refreshUsageHandler(reqpath, &globals)
            if err0 == nil {
                payload["total"] = res.Total
            } else {
                reportable_err = err0
            }

        } else if strings.HasPrefix(reqtype, "set_permissions-") {
            reportable_err = setPermissionsHandler(reqpath, &globals)
        } else if strings.HasPrefix(reqtype, "approve_probation-") {
            reportable_err = approveProbationHandler(reqpath, &globals)
        } else if strings.HasPrefix(reqtype, "reject_probation-") {
            reportable_err = rejectProbationHandler(reqpath, &globals)
        } else if strings.HasPrefix(reqtype, "create_project-") {
            reportable_err = createProjectHandler(reqpath, &globals)
        } else if strings.HasPrefix(reqtype, "delete_project-") {
            reportable_err = deleteProjectHandler(reqpath, &globals)
        } else if strings.HasPrefix(reqtype, "delete_asset-") {
            reportable_err = deleteAssetHandler(reqpath, &globals)
        } else if strings.HasPrefix(reqtype, "delete_version-") {
            reportable_err = deleteVersionHandler(reqpath, &globals)
        } else if strings.HasPrefix(reqtype, "health_check-") {
            reportable_err = nil
        } else {
            dumpErrorResponse(w, http.StatusBadRequest, "invalid request type", reqpath)
            return 
        }

        if reportable_err == nil {
            payload["status"] = "SUCCESS"
            dumpJsonResponse(w, http.StatusOK, &payload, path)
        } else {
            status_code := http.StatusInternalServerError
            var http_err *httpError
            if errors.As(reportable_err, &http_err) {
                status_code = http_err.Status
            }
            dumpErrorResponse(w, status_code, reportable_err.Error(), path)
        }
    })

    // Adding a per-day job that purges various old files.
	ticker := time.NewTicker(time.Hour * 24)
	defer ticker.Stop()

    go func() {
        for {
            <-ticker.C
            err := purgeOldFiles(staging, time.Hour * 24 * 7)
            if err != nil {
                log.Println(err)
            }

            err = purgeOldFiles(log_dir, time.Hour * 24 * 7)
            if err != nil {
                log.Println(err)
            }
        }
    }()

    // Setting up the API.
    http.ListenAndServe("0.0.0.0:" + strconv.Itoa(*port), nil)
}
