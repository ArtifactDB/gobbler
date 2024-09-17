package main

import (
    "log"
    "flag"
    "path/filepath"
    "time"
    "os"
    "errors"
    "strings"
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

func dumpHttpErrorResponse(w http.ResponseWriter, err error, path string) {
    status_code := http.StatusInternalServerError
    var http_err *httpError
    if errors.As(err, &http_err) {
        status_code = http_err.Status
    }
    message := err.Error()
    log.Printf("failed to process %q; %s\n", path, message)
    dumpJsonResponse(w, status_code, map[string]interface{}{ "status": "ERROR", "reason": message }, path)
}

func configureCors(w http.ResponseWriter, r *http.Request) bool {
    if r.Method == "OPTIONS" {
        w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
        w.Header().Set("Access-Control-Allow-Origin", "*")
        w.Header().Set("Access-Control-Allow-Headers", "*")
        w.WriteHeader(http.StatusNoContent)
        return true
    } else {
        return false
    }
}

/***************************************************/

func main() {
    spath := flag.String("staging", "", "Path to the staging directory")
    rpath := flag.String("registry", "", "Path to the registry")
    mstr := flag.String("admin", "", "Comma-separated list of administrators (default \"\")")
    port := flag.Int("port", 8080, "Port to listen to API requests")
    prefix := flag.String("prefix", "", "Prefix to add to each endpoint, excluding the first and last slashes (default \"\")")
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

    actreg := newActiveRequestRegistry(11)
    const request_expiry = time.Minute
    err := prefillActiveRequestRegistry(actreg, staging, request_expiry)
    if err != nil {
        log.Fatalf("failed to prefill active request registry; %v", err)
    }

    upreg := newUploadRequestRegistry(11)

    endpt_prefix := *prefix
    if endpt_prefix != "" {
        endpt_prefix = "/" + endpt_prefix
    }

    // Creating an endpoint to trigger jobs.
    http.HandleFunc("POST " + endpt_prefix + "/new/{path}", func(w http.ResponseWriter, r *http.Request) {
        path := r.PathValue("path")
        log.Println("processing " + path)

        reqpath, err := checkRequestFile(path, staging, request_expiry)
        if err != nil {
            dumpHttpErrorResponse(w, err, path)
            return 
        }

        if !actreg.Add(path) {
            dumpHttpErrorResponse(w, newHttpError(http.StatusBadRequest, errors.New("path is already being processed")), path)
            return
        }

        var reportable_err error
        payload := map[string]interface{}{}
        reqtype := strings.TrimPrefix(path, "request-")

        if strings.HasPrefix(reqtype, "upload-") {
            token, err0 := uploadPreflightHandler(reqpath, upreg)
            if err0 == nil {
                payload["token"] = token
            } else {
                reportable_err = err0
            }

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
        } else if strings.HasPrefix(reqtype, "health_check-") { // TO-BE-DEPRECATED, see /check below.
            reportable_err = nil
        } else {
            reportable_err = newHttpError(http.StatusBadRequest, errors.New("invalid request type"))
        }

        // Purge the request file once it's processed, to reduce the potential
        // for replay attacks. For safety's sake, we only remove it from the
        // registry if the request file was properly deleted or it expired.
        err = os.Remove(reqpath)
        if err != nil {
            log.Printf("failed to purge the request file at %q; %v", path, err)
            go func() {
                time.Sleep(request_expiry)
                actreg.Remove(path)
            }()
        } else {
            actreg.Remove(path)
        }

        if reportable_err == nil {
            payload["status"] = "SUCCESS"
            dumpJsonResponse(w, http.StatusOK, &payload, path)
        } else {
            dumpHttpErrorResponse(w, reportable_err, path) 
        }
    })

    http.HandleFunc("POST " + endpt_prefix + "/upload/{path}/{token}", func(w http.ResponseWriter, r *http.Request) {
        path := r.PathValue("path")
        token := r.PathValue("token")
        err := uploadHandler(token, upreg, path, &globals)
        if err == nil {
            payload := map[string]string{ "status": "SUCCESS" }
            dumpJsonResponse(w, http.StatusOK, &payload, path)
        } else {
            dumpHttpErrorResponse(w, err, path) 
        }
    })

    // Creating an endpoint to list and serve files, for remote access to the registry.
    fs := http.FileServer(http.Dir(globals.Registry))
    fetch_endpt := endpt_prefix + "/fetch/"
    fs_stripped := http.StripPrefix(fetch_endpt, fs)
    http.HandleFunc(fetch_endpt, func(w http.ResponseWriter, r *http.Request) {
        if configureCors(w, r) {
            return
        }
        if r.Method != "GET" {
            w.WriteHeader(http.StatusMethodNotAllowed)
            return
        }
        fs_stripped.ServeHTTP(w, r)
    })

    http.HandleFunc(endpt_prefix + "/list", func(w http.ResponseWriter, r *http.Request) {
        if configureCors(w, r) {
            return
        }
        if r.Method != "GET" {
            w.WriteHeader(http.StatusMethodNotAllowed)
            return
        }

        listing, err := listFilesHandler(r, globals.Registry)
        if err != nil {
            dumpHttpErrorResponse(w, err, "list request") 
        } else {
            dumpJsonResponse(w, http.StatusOK, &listing, "list request")
        }
    })

    // Creating some useful endpoints. 
    http.HandleFunc(endpt_prefix + "/info", func(w http.ResponseWriter, r *http.Request) {
        if configureCors(w, r) {
            return
        }
        if r.Method != "GET" {
            w.WriteHeader(http.StatusMethodNotAllowed)
            return
        }
        dumpJsonResponse(w, http.StatusOK, map[string]string{ "staging": staging, "registry": globals.Registry }, "info request")
    })

    http.HandleFunc(endpt_prefix + "/", func(w http.ResponseWriter, r *http.Request) {
        if configureCors(w, r) {
            return
        }
        if r.Method != "GET" {
            w.WriteHeader(http.StatusMethodNotAllowed)
            return
        }
        dumpJsonResponse(w, http.StatusOK, map[string]string{ "name": "gobbler API", "url": "https://github.com/ArtifactDB/gobbler" }, "default request")
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
    log.Fatal(http.ListenAndServe("0.0.0.0:" + strconv.Itoa(*port), nil))
}
