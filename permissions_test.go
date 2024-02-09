package main

import (
    "testing"
    "os/user"
    "io/ioutil"
)

func TestIdentifyUser(t *testing.T) {
    dir, err := ioutil.TempDir("", "")
    if (err != nil) {
        t.Fatalf("failed to create a temporary directory; %v", err)
    }

    username, err := IdentifyUser(dir)
    if err != nil {
        t.Fatalf("failed to identify user from file; %v", err)
    }

    self, err := user.Current()
    if err != nil {
        t.Fatalf("failed to identify current user; %v", err)
    }

    if username != self.Username {
        t.Fatalf("wrong user (expected + '" + self.Username + "', got '" + username + "')")
    }
}
