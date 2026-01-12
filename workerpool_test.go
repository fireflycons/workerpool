// Package workerpool_test contains tests for the workerpool package.
//
// Tests in this package maximize the use of goroutines to simulate
// concurrent workloads and ensure that the workerpool and its queues
// function correctly under such conditions.
package workerpool_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	cwp "github.com/cilium/workerpool"
	queue "github.com/fireflycons/concurrentqueue"
	"github.com/fireflycons/workerpool"
	"github.com/stretchr/testify/require"
)

// Same test setup for all the tests below
var tests = []struct {
	name   string
	useDlq bool
}{
	{
		// Without DLQ
		name:   "squares",
		useDlq: false,
	},
	{
		// With DLQ and 25% error rate
		name:   "squares with errors",
		useDlq: true,
	},
}

const nElements = 10000

func TestRun(t *testing.T) {

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			input, expectedOutput := makeSquares(nElements)
			mu := sync.Mutex{}
			actualOutput := make([]int, 0, nElements)

			q := queue.New[int]()
			wg := sync.WaitGroup{}

			// Job to be run by workerpool
			// One instance of job is run per queued item
			jobFunc := func(_ context.Context, v int) error {

				// Simulate errors when we have a DLQ
				if tt.useDlq && random25PercentError() {
					return fmt.Errorf("simulated error for value %d", v)
				}

				mu.Lock()
				actualOutput = append(actualOutput, v*v)
				mu.Unlock()
				return nil
			}

			// Goroutine to populate input queue
			wg.Go(func() {
				for _, v := range input {
					_ = q.Enqueue(v)
				}

				// Must close here or Run will block forever.
				q.Close()
			})

			// Goroutine to run the workerpool
			var taskOutputs []cwp.Task
			var dlq = func() *queue.Queue[int] {
				if tt.useDlq {
					return queue.New[int]()
				}
				return nil
			}()

			wg.Go(func() {
				var err error
				taskOutputs, err = workerpool.Run(
					jobFunc,
					q,
					workerpool.WithContext[int](t.Context()),
					workerpool.WithDeadLetterQueue(dlq),
				)

				// The pool itself should not error, even if some jobs did.
				require.NoError(t, err)
			})

			wg.Wait()

			if tt.useDlq {

				require.InDelta(t, nElements/4, dlq.Len(), nElements/20, "Expected around 25%% errors but was %d%%", dlq.Len()*100/nElements)
				require.Equal(t, dlq.Len(), countTaskErrors(taskOutputs), "DLQ length should match number of errors")
				dlq.Close()
				dlq.Drain()

			} else {
				require.ElementsMatch(t, expectedOutput, actualOutput)
				require.Equal(t, 0, countTaskErrors(taskOutputs), "No errors expected when DLQ is not used")
			}
		})
	}
}

func TestRunWithResults(t *testing.T) {

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {

			input, expectedOutput := makeSquares(nElements)
			actualOutput := make([]int, 0, nElements)

			resultsChan := make(chan int, 100)
			q := queue.New[int]()
			wg := sync.WaitGroup{}

			// Job to be run by workerpool
			// One instance of job is run per queued item
			jobFunc := func(_ context.Context, v int, results chan<- int) error {

				// Simulate errors when we have a DLQ
				if tt.useDlq && random25PercentError() {
					return fmt.Errorf("simulated error for value %d", v)
				}

				results <- v * v
				return nil
			}

			// Goroutine to populate input queue
			wg.Go(func() {
				fillInputQueue(t, q, input)
			})

			// Goroutine to run the workerpool
			var taskOutputs []cwp.Task
			var dlq = func() *queue.Queue[int] {
				if tt.useDlq {
					return queue.New[int]()
				}
				return nil
			}()

			wg.Go(func() {
				var err error
				taskOutputs, err = workerpool.RunWithResults(
					jobFunc,
					q,
					resultsChan,
					workerpool.WithContext[int](t.Context()),
					workerpool.WithDeadLetterQueue(dlq),
				)
				close(resultsChan)

				require.NoError(t, err)
			})

			wg.Go(func() {
				for v := range resultsChan {
					actualOutput = append(actualOutput, v)
				}
			})

			wg.Wait()

			if tt.useDlq {

				require.InDelta(t, nElements/4, dlq.Len(), nElements/20, "Expected around 25%% errors but was %d%%", dlq.Len()*100/nElements)
				require.Equal(t, dlq.Len(), countTaskErrors(taskOutputs), "DLQ length should match number of errors")
				dlq.Close()
				dlq.Drain()

			} else {
				require.ElementsMatch(t, expectedOutput, actualOutput)
				require.Equal(t, 0, countTaskErrors(taskOutputs), "No errors expected when DLQ is not used")
			}
		})
	}
}

func TestRunWorkers(t *testing.T) {

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			input, expectedOutput := makeSquares(nElements)
			mu := sync.Mutex{}
			actualOutput := make([]int, 0, nElements)

			q := queue.New[int]()
			wg := sync.WaitGroup{}

			// Job to be run by workerpool
			// One instance of job is run per workerpool slot (defaults to runtime.NumCPU())
			jobFunc := func(_ context.Context, q <-chan int, dlq *queue.Queue[int]) error { //nolint:unparam
				for v := range q {
					// Simulate errors when we have a DLQ
					if dlq != nil && random25PercentError() {
						_ = dlq.Enqueue(v)
						continue
					}
					mu.Lock()
					actualOutput = append(actualOutput, v*v)
					mu.Unlock()
				}

				// It is up to the developer whether or not to return
				// an error here if any input item caused an error.
				return nil
			}

			// Goroutine to fill the input queue
			wg.Go(func() {
				fillInputQueue(t, q, input)
			})

			// Goroutine to run the workerpool
			var taskOutputs []cwp.Task

			var dlq = func() *queue.Queue[int] {
				if tt.useDlq {
					return queue.New[int]()
				}
				return nil
			}()

			wg.Go(func() {
				var err error
				taskOutputs, err = workerpool.RunWorkers(
					jobFunc,
					q,
					workerpool.WithContext[int](t.Context()),
					workerpool.WithDeadLetterQueue(dlq),
				)

				require.NoError(t, err)
			})
			wg.Wait()

			if tt.useDlq {

				require.InDelta(t, nElements/4, dlq.Len(), nElements/20, "Expected around 25%% errors but was %d%%", dlq.Len()*100/nElements)
				// jobFunc did not return an error
				require.Equal(t, 0, countTaskErrors(taskOutputs), "DLQ length should match number of errors")
				dlq.Close()
				dlq.Drain()

			} else {
				require.ElementsMatch(t, expectedOutput, actualOutput)
				require.Equal(t, 0, countTaskErrors(taskOutputs), "No errors expected when DLQ is not used")
			}
		})
	}
}

func TestRunWorkersWithResults(t *testing.T) {

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			input, expectedOutput := makeSquares(nElements)
			actualOutput := make([]int, 0, nElements)

			resultsChan := make(chan int, 100)
			q := queue.New[int]()
			wg := sync.WaitGroup{}

			// Job to be run by workerpool
			// One instance of job is run per workerpool slot (defaults to runtime.NumCPU())
			jobFunc := func(_ context.Context, q <-chan int, results chan<- int, dlq *queue.Queue[int]) error {
				for v := range q {
					// Simulate errors when we have a DLQ
					if dlq != nil && random25PercentError() {
						_ = dlq.Enqueue(v)
						continue
					}
					results <- v * v
				}

				// It is up to the developer whether or not to return
				// an error here if any input item caused an error.
				return nil
			}

			/// Goroutine to fill the input queue
			wg.Go(func() {
				fillInputQueue(t, q, input)
			})

			// Goroutine to run the workerpool
			var taskOutputs []cwp.Task
			var dlq = func() *queue.Queue[int] {
				if tt.useDlq {
					return queue.New[int]()
				}
				return nil
			}()

			wg.Go(func() {
				var err error
				taskOutputs, err = workerpool.RunWorkersWithResults(
					jobFunc,
					q,
					resultsChan,
					workerpool.WithContext[int](t.Context()),
					workerpool.WithDeadLetterQueue(dlq),
				)

				// Close output when all workers finish writing
				close(resultsChan)
				require.NoError(t, err)
			})

			// Goroutine to collect the results
			wg.Go(func() {
				for v := range resultsChan {
					actualOutput = append(actualOutput, v)
				}
			})

			wg.Wait()

			if tt.useDlq {

				require.InDelta(t, nElements/4, dlq.Len(), nElements/20, "Expected around 25%% errors but was %d%%", dlq.Len()*100/nElements)
				// jobFunc did not return an error
				require.Equal(t, 0, countTaskErrors(taskOutputs), "DLQ length should match number of errors")
				dlq.Close()
				dlq.Drain()

			} else {
				require.ElementsMatch(t, expectedOutput, actualOutput)
				require.Equal(t, 0, countTaskErrors(taskOutputs), "No errors expected when DLQ is not used")
			}
		})
	}
}

// makeSquares creates input and expected output slices for testing.
// Input contains integers from 0 to nElements-1.
// Output contains the squares of the input integers.
func makeSquares(nElements int) (input, output []int) { //nolint:unparam

	input = make([]int, nElements)
	output = make([]int, nElements)
	for i := 0; i < nElements; i++ {
		input[i] = i
		output[i] = i * i
	}
	return input, output
}

// fillInputQueue enqueues all items from input into the provided queue and closes it.
func fillInputQueue[T any](t *testing.T, q *queue.Queue[T], input []T) {
	t.Helper()

	for _, v := range input {
		require.NoError(t, q.Enqueue(v), "Enqueue should not error")
	}

	q.Close()
}

// 25% chance of error
func random25PercentError() bool {
	return rand.Intn(4) == 0
}

func countTaskErrors(tasks []cwp.Task) int {
	errCount := 0
	for _, t := range tasks {
		if t.Err() != nil {
			errCount++
		}
	}
	return errCount
}
