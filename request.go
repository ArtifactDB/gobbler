package main

import (
    "fmt"
    "os"
    "encoding/json"
)

type UploadRequest struct {
    Self string
    Source *string `json:"source"`
    Prefix *string `json:"prefix"`
    Project *string `json:"project"`
    Asset *string `json:"asset"`
    Version *string `json:"version"`
    Permissions *Permissions `json:"permissions"`
    OnProbation *bool `json:"on_probation"`
}

func ReadUploadRequest(path string) (*UploadRequest, error) {
    handle, err := os.ReadFile(path)
    if err != nil {
        return nil, fmt.Errorf("failed to read %q; %w", path, err)
    }

    var details UploadRequest
    err = json.Unmarshal(handle, &details)
    if err != nil {
        return nil, fmt.Errorf("failed to parse JSON from %q; %w", path, err)
    }

    if details.Source == nil {
        return nil, fmt.Errorf("expected a 'source' property in %q; %w", path, err)
    }
    source := *(details.Source)
    source_user, err := IdentifyUser(source)
    if err != nil {
        return nil, fmt.Errorf("failed to find owner of %q; %w", source, err)
    }

    req_user, err := IdentifyUser(path)
    if err != nil {
        return nil, fmt.Errorf("failed to find owner of %q; %w", path, err)
    }
    if source_user != req_user {
        return nil, fmt.Errorf("requesting user must be the same as the owner of the 'source' directory (%s vs %s)", source_user, req_user)
    }

    details.Self = path
    return &details, nil
}
