package main

type concurrencyThrottle struct {
    Available chan int
}

func newConcurrencyThrottle(max_concurrency int) concurrencyThrottle {
    output := concurrencyThrottle{ Available: make(chan int, max_concurrency) }
    for i := range max_concurrency {
        output.Available <- i
    }
    return output
}

func (ct *concurrencyThrottle) Wait() int {
    return <-ct.Available
}

func (ct *concurrencyThrottle) Release(x int) {
    ct.Available <- x
}
