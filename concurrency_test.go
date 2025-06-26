package main

import (
    "testing"
    "sync"
    "sort"
)

func TestConcurrencyThrottle(t *testing.T) {
    throttle := newConcurrencyThrottle(5)

    collected := []int{}
    for _ = range 5 {
        collected = append(collected, throttle.Wait())
    }
    sort.Ints(collected)
    for i := range 5 {
        if collected[i] != i {
            t.Error("unexpected number of concurrency handles")
        }
    }

    foo := -1
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        foo = throttle.Wait()
    }()
    if foo != -1 {
        t.Error("expected the throttle to still be waiting")
    }

    throttle.Release(3)
    wg.Wait()
    if foo != 3 {
        t.Error("expected the throttle to reacquire after the wait")
    }
}
