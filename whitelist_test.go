package main

import (
    "testing"
    "strings"
    "os"
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
}

func TestLoadLinkWhitelist(t *testing.T) {
    t.Run("okay", func(t *testing.T) {
        other, err := os.CreateTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        message := "/alpha/\n/bravo/\n/charlie/delta/"
        if _, err := other.WriteString(message); err != nil {
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

        if len(loaded) != 3 || loaded[0] != "/alpha/" || loaded[1] != "/bravo/" || loaded[2] != "/charlie/delta/" {
            t.Error("unexpected content from the loaded whitelist file")
        }
    })

    t.Run("okay", func(t *testing.T) {
        other, err := os.CreateTemp("", "")
        if err != nil {
            t.Fatal(err)
        }

        message := "/alpha/\n/bravo\n/charlie/delta/"
        if _, err := other.WriteString(message); err != nil {
            t.Fatal(err)
        }
        other_name := other.Name()
        if err := other.Close(); err != nil {
            t.Fatal(err)
        }

        _, err = loadLinkWhitelist(other_name)
        if err == nil || !strings.Contains(err.Error(), "end with a path separator") {
            t.Errorf("expected failure when there is no path limiter; %v", err)
        }
    })
}
