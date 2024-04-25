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
    "io/fs"
    "syscall"
    "sync"
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

func checkRequestFile(path, staging string) (string, error) {
    if !strings.HasPrefix(path, "request-") {
        return "", newHttpError(http.StatusBadRequest, errors.New("file name should start with \"request-\""))
    }

    if !filepath.IsLocal(path) {
        return "", newHttpError(http.StatusBadRequest, errors.New("path should be local to the staging directory"))
    }
    reqpath := filepath.Join(staging, path)

    info, err := os.Lstat(reqpath)
    if err != nil {
        return "", newHttpError(http.StatusBadRequest, fmt.Errorf("failed to access path; %v", err))
    }

    if info.IsDir() {
        return "", newHttpError(http.StatusBadRequest, errors.New("path is a directory"))
    }

    if info.Mode() & fs.ModeSymlink != 0 {
        return "", newHttpError(http.StatusBadRequest, errors.New("path is a symbolic link"))
    }

    s, ok := info.Sys().(*syscall.Stat_t)
    if !ok {
        return "", fmt.Errorf("failed to convert to a syscall.Stat_t; %w", err)
    }
    if uint32(s.Nlink) > 1 {
        return "", newHttpError(http.StatusBadRequest, errors.New("path seems to have multiple hard links"))
    }

    return reqpath, nil
}

/***************************************************/

// This tracks the requests that are currently being processed, to prevent the
// same request being processed multiple times at the same time. We use a
// multi-pool approach to improve parallelism across requests.
type activeRegistry struct {
    NumPools int
    Locks []sync.Mutex
    Active []map[string]bool
}

func newActiveRegistry(num_pools int) *activeRegistry {
    return &activeRegistry {
        NumPools: num_pools,
        Locks: make([]sync.Mutex, num_pools),
        Active: make([]map[string]bool, num_pools),
    }
}

func (a *activeRegistry) choosePool(path string) int {
    sum := 0
    for _, r := range path {
        sum += int(r)
    }
    return sum % a.NumPools
}

func (a *activeRegistry) Add(path string) bool {
    i := a.choosePool(path)
    a.Locks[i].Lock()
    defer a.Locks[i].Unlock()

    if a.Active[i] == nil {
        a.Active[i] = map[string]bool{}
    } else {
        _, ok := a.Active[i][path]
        if ok {
            return false
        }
    }
   
    a.Active[i][path] = true
    return true
}

func (a *activeRegistry) Remove(path string) {
    i := a.choosePool(path)
    a.Locks[i].Lock()
    defer a.Locks[i].Unlock()
    delete(a.Active[i], path)
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

    actreg := newActiveRegistry(11)

    endpt_prefix := *prefix
    if endpt_prefix != "" {
        endpt_prefix = "/" + endpt_prefix
    }

    // Creating an endpoint to trigger jobs.
    http.HandleFunc("POST " + endpt_prefix + "/new/{path}", func(w http.ResponseWriter, r *http.Request) {
        path := r.PathValue("path")
        log.Println("processing " + path)

        reqpath, err := checkRequestFile(path, staging)
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
        } else if strings.HasPrefix(reqtype, "health_check-") { // TO-BE-DEPRECATED, see /check below.
            reportable_err = nil
        } else {
            reportable_err = newHttpError(http.StatusBadRequest, errors.New("invalid request type"))
        }

        // Purge the request file once it's processed, to reduce the potential
        // for replay attacks. For safety's sake, we only remove it from the
        // registry if the request file was properly deleted.
        err = os.Remove(reqpath)
        if err != nil {
            log.Printf("failed to purge the request file at %q; %v", path, err)
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
