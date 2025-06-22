package main

import (
    "testing"
    "os"
    "os/user"
    "io/ioutil"
    "errors"
    "strings"
)

func TestIsSpoofingAllowed(t *testing.T) {
    spoof_perms := map[string]spoofPermissions{}

    if isSpoofingAllowed("foo", "bar", spoof_perms) {
        t.Error("should not have allowed spoofing")
    }

    spoof_perms["foo"] = spoofPermissions{ Users: map[string]bool{} }
    if isSpoofingAllowed("foo", "bar", spoof_perms) {
        t.Error("should not have allowed spoofing")
    }

    spoof_perms["foo"] = spoofPermissions{ Users: map[string]bool{ "bar": true } }
    if !isSpoofingAllowed("foo", "bar", spoof_perms) {
        t.Error("should have allowed spoofing")
    }
    if isSpoofingAllowed("foo", "whee", spoof_perms) {
        t.Error("should not have allowed spoofing")
    }

    spoof_perms["foo"] = spoofPermissions{ All: true, Users: map[string]bool{} }
    if !isSpoofingAllowed("foo", "bar", spoof_perms) {
        t.Error("should have allowed spoofing")
    }
}

func TestIdentifySpoofedUser(t *testing.T) {
    self, err := user.Current()
    if err != nil {
        t.Fatalf("failed to identify current user; %v", err)
    }

    dir, err := ioutil.TempDir("", "")
    if err != nil {
        t.Fatalf("failed to create a temporary directory; %v", err)
    }

    spoof_perms := map[string]spoofPermissions{}
    spoofed, err := identifySpoofedUser(dir, nil, spoof_perms)
    if err != nil {
        t.Fatal(err)
    } else if spoofed != self.Username {
        t.Errorf("expected user to be the current user; %q", spoofed)
    }

    candidate := self.Username + "fake"
    spoofed, err = identifySpoofedUser(dir, &candidate, spoof_perms)
    var http_err *httpError
    if err == nil || !errors.As(err, &http_err) {
        t.Errorf("user is not authorized to spoof; %v", err)
    }

    spoof_perms[self.Username] = spoofPermissions{ All: true }
    spoofed, err = identifySpoofedUser(dir, &candidate, spoof_perms)
    if err != nil {
        t.Fatal(err)
    } else if spoofed != candidate {
        t.Errorf("expected user to be the spoofed user; %q", spoofed)
    }
}

func TestLoadSpoofPermissions(t *testing.T) {
    handle, err := os.CreateTemp("", "")
    if err != nil {
        t.Fatal(err)
    }
    message := "alpha:bravo,charlie\ndelta:echo\nfoxtrot:*\ngolf:"
    if _, err := handle.WriteString(message); err != nil {
        t.Fatal(err)
    }
    fname := handle.Name()
    if err := handle.Close(); err != nil {
        t.Fatal(err)
    }

    sperms, err := loadSpoofPermissions(fname)
    if err != nil {
        t.Fatal(err)
    }
    if len(sperms) != 3 {
        t.Fatal("expected three users in the spoof permissions file")
    }

    {
        found, ok := sperms["alpha"]
        if !ok {
            t.Fatal("expected to find 'alpha' in the spoof permissions")
        }
        if found.All {
            t.Error("unexpected global spoof enabled for 'alpha'")
        }
        if len(found.Users) != 2 {
            t.Error("expected two allowed users in the spoof permissions for 'alpha'")
        }
        if _, ok := found.Users["bravo"]; !ok {
            t.Error("expected to find 'bravo' in the spoof permissions file")
        }
        if _, ok := found.Users["charlie"]; !ok {
            t.Error("expected to find 'charlie' in the spoof permissions file")
        }
    }

    {
        found, ok := sperms["delta"]
        if !ok {
            t.Fatal("expected to find 'delta' in the spoof permissions")
        }
        if found.All {
            t.Error("unexpected global spoof enabled for 'delta'")
        }
        if len(found.Users) != 1 {
            t.Error("expected two allowed users in the spoof permissions for 'delta'")
        }
        if _, ok := found.Users["echo"]; !ok {
            t.Error("expected to find 'echo' in the spoof permissions file")
        }
    }

    {
        found, ok := sperms["foxtrot"]
        if !ok {
            t.Fatal("expected to find 'foxtrot' in the spoof permissions")
        }
        if !found.All {
            t.Error("expected global spoof enabled for 'foxtrot'")
        }
        if len(found.Users) != 0 {
            t.Error("expected two allowed users in the spoof permissions for 'foxtrot'")
        }
    }

    if _, ok := sperms["golf"]; ok {
        t.Fatal("expected to skip 'golf' in the spoof permissions")
    }
}

func TestLoadSpoofPermissionsFail(t *testing.T) {
    handle, err := os.CreateTemp("", "")
    if err != nil {
        t.Fatal(err)
    }
    message := "alpha,bravo,charlie"
    if _, err := handle.WriteString(message); err != nil {
        t.Fatal(err)
    }
    fname := handle.Name()
    if err := handle.Close(); err != nil {
        t.Fatal(err)
    }

    _, err = loadSpoofPermissions(fname)
    if err == nil || !strings.Contains(err.Error(), "unexpected format") {
        t.Error("expected loading to fail with invalid line")
    }
}
