// Package workerpool prodives a wrapper around github.com/cilium/workerpool
// to facilitate processing items from a concurrentqueue.Queue using
// a pool of workers.
package workerpool

import (
	"context"
	"runtime"
	"strconv"

	cwp "github.com/cilium/workerpool"
	queue "github.com/fireflycons/concurrentqueue"
)

type poolOptions[T any] struct {
	numWorkers int
	taskNamer  func(int, T) string
}

// OptionFunc is a function that configures a worker pool.
type OptionFunc[T any] func(*poolOptions[T])

// WithNumWorkers is a constructor function to set the number of
// workers in the worker pool.
func WithNumWorkers[T any](n int) OptionFunc[T] {
	return func(o *poolOptions[T]) {
		o.numWorkers = n
	}
}

// WithTaskNamer is a constructor function to set a function that
// generates task names based on the task ID and the task input value.
//
// For RunChan, the input value will always be the zero value of T.
// This is because the task function in RunChan receives a channel
// from which it reads input values, rather than receiving the input
// value directly.
//
// The default task namer returns the string representation of the
// task ID.
func WithTaskNamer[T any](namer func(int, T) string) OptionFunc[T] {
	return func(o *poolOptions[T]) {
		o.taskNamer = namer
	}
}

// Run creates a pool of workers to run the given func in parallel.
//
// One instance of jobFunc is created for each element dequeued from q.
// The number of concurrently running instances of jobFunc is limited
// to numWorkers which defaults to the number of CPU cores available.
//
// Run returns a slice of Tasks representing the results of processing
// each element from the queue. If an error occurs during processing,
// it is returned as the second return value.
//
// The total number of tasks returned is equal to the number of items
// dequeued from the queue.
//
// The provided queue is drained before returning, regardless of whether
// an error occurred.
//
// It is the responsibility of the caller to ensure that the queue is
// populated with all intended work items before calling Run; if the
// queue is empty, no work will be performed. Similarly, if the queue
// contains more items than intended, those extra items will also be
// processed.
func Run[T any](jobFunc func(context.Context, T) error, q *queue.Queue[T], opts ...OptionFunc[T]) ([]cwp.Task, error) {

	options := poolOptions[T]{
		numWorkers: runtime.NumCPU(),
		taskNamer: func(i int, _ T) string {
			return strconv.Itoa(i)
		},
	}

	for _, o := range opts {
		o(&options)
	}

	wp := cwp.New(options.numWorkers)

	defer func() {

		// Release worker pool resources on function return.
		// Ignore error. Only possible error is "aleady closed"
		_ = wp.Close()
	}()

	// Add tasks to pool from queue
	id := 0
	for item := range q.Dequeue() {
		// Ignore error here. It would be ErrDraining or ErrClosed,
		// neither of which can occur in this usage of workerpool
		_ = wp.Submit(options.taskNamer(id, item), func(ctx context.Context) error {
			return jobFunc(ctx, item)
		})
		id++
	}

	// Wait for pool to complete
	res, err := wp.Drain()
	q.Drain()
	return res, err
}

// RunChan creates a pool of workers to run the given func in parallel.
//
// The number of instances of jobFunc is limited to numWorkers, however
// each instance of jobFunc is responsible for dequeuing work items from q.
// Thus, the number of tasks created is equal to numWorkers
// which defaults to the number of CPU cores available.
//
// RunChan returns a slice of Tasks representing the results of each worker.
// If an error occurs during processing, it is returned as the second
// return value.
//
// The provided queue is drained before returning, regardless of whether
// an error occurred.
//
// It is the responsibility of the caller to ensure that the queue is
// populated with all intended work items before calling RunChan; if the
// queue is empty, no work will be performed. Similarly, if the queue
// contains more items than intended, those extra items will also be
// processed.
func RunChan[T any](jobFunc func(context.Context, <-chan T) error, q *queue.Queue[T], opts ...OptionFunc[T]) ([]cwp.Task, error) {

	options := poolOptions[T]{
		numWorkers: runtime.NumCPU(),
		taskNamer: func(i int, _ T) string {
			return strconv.Itoa(i)
		},
	}

	for _, o := range opts {
		o(&options)
	}

	wp := cwp.New(options.numWorkers)

	defer func() {

		// Release worker pool resources on function return.
		// Ignore error. Only possible error is "aleady closed"
		_ = wp.Close()
	}()

	var zero T
	id := 0
	for range options.numWorkers {
		// Ignore error here. It would be ErrDraining or ErrClosed,
		// neither of which can occur in this usage of workerpool
		_ = wp.Submit(options.taskNamer(id, zero), func(ctx context.Context) error {
			return jobFunc(ctx, q.Dequeue())
		})
		id++
	}

	// Wait for pool to complete
	res, err := wp.Drain()
	q.Drain()
	return res, err
}
