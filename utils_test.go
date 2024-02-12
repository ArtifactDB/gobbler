package main

import (
    "testing"
    "strings"
)

func TestIsBadName(t *testing.T) {
    var err error

    err = isBadName("..foo")
    if err == nil || !strings.Contains(err.Error(), "..")  {
        t.Fatal("failed to stop on '..'")
    }

    err = isBadName("")
    if err == nil || !strings.Contains(err.Error(), "empty") {
        t.Fatal("failed to stop on an empty name")
    }

    err = isBadName("asda/a")
    if err == nil || !strings.Contains(err.Error(), "/") {
        t.Fatal("failed to stop in the presence of a forward slash")
    }

    err = isBadName("asda\\asdasd")
    if err == nil || !strings.Contains(err.Error(), "\\") {
        t.Fatal("failed to stop in the presence of a backslash")
    }
}
