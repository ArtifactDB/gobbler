package main

import (
    "time"
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

func DumpSuccessLog(path, project, version string) error {
    payload := map[string]string{}
    payload["status"] = "SUCCESS"
    payload["project"] = project
    payload["version"] = version
    deets, _ := json.MarshalIndent(&payload, "", "    ")
    return os.WriteFile(path, deets, 0644)
}

func DumpVersionMetadata(path, user string) error {
    payload := map[string]string{}
    payload["uploader_id"] = user
    payload["uploaded_at"] = time.Now().Format(time.RFC3339)
    deets, _ := json.MarshalIndent(&payload, "", "    ")
    return os.WriteFile(path, deets, 0644)
}
