package main

import (
    "testing"
    "os"
    "strings"
)

func TestIsLinkWhitelisted(t *testing.T) {
    if !isLinkWhitelisted("/foo/bar", []string{ "/foo/" }) {
        t.Error("expected link to be whitelisted")
    }

    if !isLinkWhitelisted("/foo/bar", []string{ "/bar/", "/foo/" }) {
        t.Error("expected link to be whitelisted")
    }

    if isLinkWhitelisted("/foo/bar", []string{ "/bar/" }) {
        t.Error("expected link to not be whitelisted")
    }

    // Still works if nil.
    if isLinkWhitelisted("/foo/bar", nil) {
        t.Error("expected link to be whitelisted")
    }
}

func TestLoadLinkWhitelist(t *testing.T) {
    other, err := os.CreateTemp("", "")
    if err != nil {
        t.Fatal(err)
    }

    if _, err := other.WriteString("/alpha/\n/bravo/.\n/charlie//delta/"); err != nil {
        t.Fatal(err)
    }
    other_name := other.Name()
    if err := other.Close(); err != nil {
        t.Fatal(err)
    }

    loaded, err := loadLinkWhitelist(other_name)
    if err != nil {
        t.Fatal(err)
    }

    if len(loaded) != 3 || loaded[0] != "/alpha" || loaded[1] != "/bravo" || loaded[2] != "/charlie/delta" {
        t.Error("unexpected content from the loaded whitelist file")
    }

    if err := os.WriteFile(other_name, []byte("alpha/bravo/charlie"), 0644); err != nil {
        t.Fatal(err)
    }
    _, err = loadLinkWhitelist(other_name)
    if err == nil || !strings.Contains(err.Error(), "absolute") {
        t.Error("expected an error when paths are not absolute")
    }
}
