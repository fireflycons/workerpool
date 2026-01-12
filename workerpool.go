// Package workerpool provides a wrapper around [github.com/cilium/workerpool]
// to facilitate processing items from a [concurrentqueue.Queue] using
// a pool of workers.
//
// One caveat is that while the number of concurrently running workers is limited,
// task results are not and they accumulate until they are collected. This will be
// the case when using the Run rather than the RunWorkers methods since one task
// is created per queue item. For large queues this may lead to high memory usage,
// so it is advisable to use the RunWorkers methods where possible.
//
// [github.com/cilium/workerpool]: https://pkg.go.dev/github.com/cilium/workerpool
// [concurrentqueue.Queue]: https://github.com/fireflycons/concurrentqueue
package workerpool

import (
	"context"
	"errors"
	"runtime"
	"strconv"

	cwp "github.com/cilium/workerpool"
	queue "github.com/fireflycons/concurrentqueue"
)

type poolOptions[T any] struct {
	ctx        context.Context
	numWorkers int
	taskNamer  func(int, T) string
	dlq        *queue.Queue[T]
}

// OptionFunc is a function that configures a worker pool.
type OptionFunc[T any] func(*poolOptions[T])

// WithNumWorkers is a constructor function to set the number of
// workers in the worker pool. The default is the number of
// CPU cores available.
//
// The underlying worker pool will panic if n ≤ 0.
func WithNumWorkers[T any](n int) OptionFunc[T] {
	return func(o *poolOptions[T]) {
		o.numWorkers = n
	}
}

// WithTaskNamer is a constructor function to set a function that
// generates task names based on the task ID and the task input value.
//
// For RunWorkers methods, the input value will always be the zero value of T.
// This is because the task function in these methods receives a channel
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

// WithContext is a constructor function to set the parent context
// for the worker pool.
//
// This context is passed to all workers, which should return
// promptly when the context is cancelled.
//
// If no context is provided, context.Background() is used.
func WithContext[T any](ctx context.Context) OptionFunc[T] {
	return func(o *poolOptions[T]) {
		o.ctx = ctx
	}
}

// WithDeadLetterQueue is a constructor function to set a dead letter queue
// for failed tasks.
//
// How this queue is populated depends on which Run method you are using.
// See the documentation for each Run method for details.
func WithDeadLetterQueue[T any](dlq *queue.Queue[T]) OptionFunc[T] {
	return func(o *poolOptions[T]) {
		o.dlq = dlq
	}
}

// Run creates a pool of workers to run the given jobFunc in parallel.
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
// If a dead letter queue is provided via WithDeadLetterQueue, the currently
// processed item will be added to it if jobFunc returns an error.
//
// The provided queue is drained before returning, regardless of whether
// an error occurred. Any remaining items will be added to the dead letter queue
// if one is provided.
//
// It is the responsibility of the caller to ensure that the queue is
// populated with all intended work items before calling Run; if the
// queue is empty, no work will be performed. Similarly, if the queue
// contains more items than intended, those extra items will also be
// processed.
func Run[T any](
	jobFunc func(context.Context, T) error,
	q *queue.Queue[T],
	opts ...OptionFunc[T]) ([]cwp.Task, error) {

	wp, options := makePool(opts...)

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
			err := jobFunc(ctx, item)
			if err != nil && options.dlq != nil {
				if qerr := options.dlq.Enqueue(item); qerr != nil {
					return errors.Join(err, qerr)
				}
			}
			return err
		})
		id++
	}

	return drainAndReturn(wp, q, options)
}

// RunWithResults creates a pool of workers to run the given jobFunc in parallel
// providing an output channel for results.
//
// Type I is the input type dequeued from q and type O is the output (result) type from
// jobFunc which is sent to the out channel. Caller must ensure out channel is
// properly buffered or consumed to avoid deadlock.
//
// One instance of jobFunc is created for each element dequeued from q.
// The number of concurrently running instances of jobFunc is limited
// to numWorkers which defaults to the number of CPU cores available.
//
// RunWithResults returns a slice of Tasks representing the results of processing
// each element from the queue. If an error occurs during processing,
// it is returned as the second return value.
//
// The total number of tasks returned is equal to the number of items
// dequeued from the queue.
//
// If a dead letter queue is provided via WithDeadLetterQueue, the currently
// processed item will be added to it if jobFunc returns an error.
//
// The provided queue is drained before returning, regardless of whether
// an error occurred. Any remaining items will be added to the dead letter queue
// if one is provided.
//
// It is the responsibility of the caller to ensure that the queue is
// populated with all intended work items before calling RunWithResults; if the
// queue is empty, no work will be performed. Similarly, if the queue
// contains more items than intended, those extra items will also be
// processed.
func RunWithResults[I any, O any](
	jobFunc func(context.Context, I, chan<- O) error, q *queue.Queue[I],
	out chan<- O,
	opts ...OptionFunc[I]) ([]cwp.Task, error) {

	wp, options := makePool(opts...)

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
			err := jobFunc(ctx, item, out)
			if err != nil && options.dlq != nil {
				// Send failed item to dead letter queue
				if qerr := options.dlq.Enqueue(item); qerr != nil {
					return errors.Join(err, qerr)
				}
			}
			return err
		})
		id++
	}

	return drainAndReturn(wp, q, options)
}

// RunWorkers creates a pool of workers to run the given jobFunc in parallel.
//
// Type I is the input type dequeued from q and type O is the output type from
// jobFunc which is sent to the out channel. Caller must ensure out channel is
// properly buffered or consumed to avoid deadlock.
//
// The number of instances of jobFunc is limited to numWorkers, however
// each instance of jobFunc is responsible for dequeuing work items from q.
// Thus, the number of tasks created is equal to numWorkers
// which defaults to the number of CPU cores available.
//
// RunWorkers returns a slice of Tasks representing the results of each worker.
// If an error occurs during processing, it is returned as the second
// return value.
//
// If a dead letter queue is provided via WithDeadLetterQueue, then the dlq
// argument to jobFunc will be non-nil. It is then the responsibility of
// jobFunc to enqueue any failed items to the dlq.
//
// The total number of tasks returned is equal to the number of workers.
// The provided queue is drained before returning, regardless of whether
// an error occurred.
//
// The provided queue is drained before returning, regardless of whether
// an error occurred. Any remaining items will be added to the dead letter queue
// if one is provided.
//
// It is the responsibility of the caller to ensure that the queue is
// populated with all intended work items before calling RunWorkers; if the
// queue is empty, no work will be performed. Similarly, if the queue
// contains more items than intended, those extra items will also be
// processed.
func RunWorkers[T any](
	jobFunc func(ctx context.Context, elems <-chan T, dlq *queue.Queue[T]) error,
	q *queue.Queue[T],
	opts ...OptionFunc[T]) ([]cwp.Task, error) {

	wp, options := makePool(opts...)

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
			return jobFunc(ctx, q.Dequeue(), options.dlq)
		})
		id++
	}

	return drainAndReturn(wp, q, options)
}

// RunWorkersWithResults creates a pool of workers to run the given jobFunc in parallel
// providing an output channel for results.
//
// Type I is the input type dequeued from q and type O is the output (result) type from
// jobFunc which is sent to the out channel. Caller must ensure out channel is
// properly buffered or consumed to avoid deadlock.
//
// One instance of jobFunc is created for each element dequeued from q.
// The number of concurrently running instances of jobFunc is limited
// to numWorkers which defaults to the number of CPU cores available.
//
// RunWorkersWithResults returns a slice of Tasks representing the results of processing
// each element from the queue. If an error occurs during processing,
// it is returned as the second return value.
//
// If a dead letter queue is provided via WithDeadLetterQueue, then the dlq
// argument to jobFunc will be non-nil. It is then the responsibility of
// jobFunc to enqueue any failed items to the dlq.
//
// The total number of tasks returned is equal to the number of workers.
// The provided queue is drained before returning, regardless of whether
// an error occurred.
//
// The provided queue is drained before returning, regardless of whether
// an error occurred. Any remaining items will be added to the dead letter queue
// if one is provided.
//
// It is the responsibility of the caller to ensure that the queue is
// populated with all intended work items before calling RunWorkersWithResults; if the
// queue is empty, no work will be performed. Similarly, if the queue
// contains more items than intended, those extra items will also be
// processed.
func RunWorkersWithResults[I any, O any](
	jobFunc func(ctx context.Context, in <-chan I, out chan<- O, dlq *queue.Queue[I]) error,
	q *queue.Queue[I],
	out chan<- O,
	opts ...OptionFunc[I]) ([]cwp.Task, error) {

	wp, options := makePool(opts...)

	defer func() {

		// Release worker pool resources on function return.
		// Ignore error. Only possible error is "aleady closed"
		_ = wp.Close()
	}()

	var zero I
	id := 0
	for range options.numWorkers {
		// Ignore error here. It would be ErrDraining or ErrClosed,
		// neither of which can occur in this usage of workerpool
		_ = wp.Submit(options.taskNamer(id, zero), func(ctx context.Context) error {
			return jobFunc(ctx, q.Dequeue(), out, options.dlq)
		})
		id++
	}

	return drainAndReturn(wp, q, options)
}

// makePool creates and configures a worker pool based on the provided options,
// returning the created worker pool and the final pool options.
func makePool[T any](opts ...OptionFunc[T]) (*cwp.WorkerPool, poolOptions[T]) {

	options := poolOptions[T]{
		ctx:        context.Background(),
		numWorkers: runtime.NumCPU(),
		taskNamer: func(i int, _ T) string {
			return strconv.Itoa(i)
		},
	}

	for _, o := range opts {
		o(&options)
	}

	return cwp.NewWithContext(options.ctx, options.numWorkers), options
}

// drainAndReturn drains the worker pool and the provided queue,
// returning the results from the worker pool drain.
//
// If a dead letter queue is provided in options, remaining
// items in the queue are added to it.
func drainAndReturn[T any](wp *cwp.WorkerPool, q *queue.Queue[T], options poolOptions[T]) ([]cwp.Task, error) {

	// Wait for pool to complete
	res, err := wp.Drain()
	if options.dlq != nil {
		// Drain unprocessed elements to dead letter queue
		q.DrainTo(options.dlq)
	} else {
		q.Drain()
	}
	return res, err
}
