package main

import (
    "os"
    "encoding/json"
    "fmt"
    "path/filepath"
)

const SummaryFileName = "..summary"

type SummaryMetadata struct {
    UploadUserId string `json:"upload_user_id"`
    UploadStart string `json:"upload_start"`
    UploadFinish string `json:"upload_finish"`
    OnProbation *bool `json:"on_probation,omitempty"`
}

func (s SummaryMetadata) IsProbational() bool {
    return s.OnProbation != nil && *(s.OnProbation)
}

func ReadSummary(path string) (*SummaryMetadata, error) {
    summary_path := filepath.Join(path, SummaryFileName)

    summary_raw, err := os.ReadFile(summary_path)
    if err != nil {
        return nil, fmt.Errorf("cannot read '" + summary_path + "'; %w", err)
    }

    var info SummaryMetadata
    err = json.Unmarshal(summary_raw, &info)
    if err != nil {
        return nil, fmt.Errorf("cannot parse JSON in '" + summary_path + "'; %w", err)
    }

    return &info, nil
}
