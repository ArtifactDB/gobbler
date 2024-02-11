package main

import (
    "encoding/json"
    "os"
)

func DumpFailureLog(path string, failure error) error {
    payload := map[string]string{}
    payload["status"] = "FAILED"
    payload["reason"] = failure.Error()
    deets, _ := json.MarshalIndent(&payload, "", "    ")
    return os.WriteFile(path, deets, 0644)
}

func DumpUploadSuccessLog(path, project, version string) error {
    payload := map[string]string{}
    payload["status"] = "SUCCESS"
    payload["project"] = project
    payload["version"] = version
    deets, _ := json.MarshalIndent(&payload, "", "    ")
    return os.WriteFile(path, deets, 0644)
}

func TouchSuccessLog(path string) error {
    return os.WriteFile(path, []byte(""), 0644)
}
