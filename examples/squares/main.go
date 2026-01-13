package main

import (
	"context"
	"fmt"
	"sync"

	queue "github.com/fireflycons/concurrentqueue"
	"github.com/fireflycons/workerpool"
)

type result struct {
	input  int
	square int
}

func (r result) String() string {
	return fmt.Sprintf("input: %d, square: %d", r.input, r.square)
}

func main() {

	q := queue.New[int]()

	for i := range 100 {
		err := q.Enqueue(i)
		if err != nil {
			fmt.Printf("Enqueue error: %v\n", err)
		}
	}

	q.Close()

	resultsChan := make(chan result, 10)

	wg := sync.WaitGroup{}
	ctx := context.Background() // You would use a more meaningful context

	wg.Go(func() {
		_, err := workerpool.RunWorkersWithResults(
			func(ctx context.Context, in <-chan int, out chan<- result, _ *queue.Queue[int]) error {
				for v := range in {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						// If a dead letter queue is provided (last argument), failed items would be sent there
						out <- result{input: v, square: v * v}
					}
				}
				return nil
			},
			q,
			resultsChan,
			workerpool.WithContext[int](ctx),  // Default is context.Background() if this option is not provided.
			workerpool.WithNumWorkers[int](4), // Default is runtime.NumCPU() if this option is not provided.
		)
		close(resultsChan)

		if err != nil {
			panic(fmt.Sprintf("Error starting workers: %v\n", err))
		}
	})

	wg.Go(func() {
		for r := range resultsChan {
			fmt.Println(r)
		}
	})

	wg.Wait()
}
