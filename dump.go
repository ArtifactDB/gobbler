package main

import (
    "encoding/json"
    "os"
    "fmt"
)

func DumpFailureLog(path string, failure error) error {
    payload := map[string]string{}
    payload["status"] = "FAILED"
    payload["reason"] = failure.Error()
    return dump_json(path, payload)
}

func DumpUploadSuccessLog(path, project, version string) error {
    payload := map[string]string{}
    payload["status"] = "SUCCESS"
    payload["project"] = project
    payload["version"] = version
    return dump_json(path, payload)
}

func TouchSuccessLog(path string) error {
    return os.WriteFile(path, []byte(""), 0644)
}

func dump_json(path string, output interface{}) error {
    str, err := json.MarshalIndent(output, "", "    ")
    if err != nil {
        return fmt.Errorf("failed to stringify output to JSON; %w", err)
    }

    err = os.WriteFile(path, str, 0644)
    if err != nil {
        return fmt.Errorf("failed to write to %q; %w", path, err)
    }

    return nil
}
