package main

import (
	"fmt"
	"sync"
	"time"
)

type Job struct {
	id         int
	jobType    string
	payload    string
	status     string
	createdAt  time.Time
	maxRetries int
	retryCount int
}

type jobQueue struct {
	queue chan Job
	wg    sync.WaitGroup
}

func newQueue(size int) *jobQueue {
	return &jobQueue{
		queue: make(chan Job, size),
	}
}

func (q *jobQueue) enqueue(job Job) {
	q.wg.Add(1)
	q.queue <- job
}

func (q *jobQueue) processJob(job Job) error {
	time.Sleep(1 * time.Second) // Simulate job processing time
	fmt.Printf("Processing job: %s, payload: %s\n", job.jobType, job.payload)
	return nil
}

func (q *jobQueue) startWorkers(workers int) {
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			for job := range q.queue {
				fmt.Printf("Worker: %d\n", workerID)
				if err := q.processJob(job); err != nil {
					fmt.Printf("Worker: %d failed to process job: %d\n", workerID, job.id)
					if job.retryCount < job.maxRetries {
						job.retryCount++
						q.enqueue(job)
					} else {
						fmt.Printf("Worker: %d job: %d reached max retries\n", workerID, job.id)
					}
				}
				q.wg.Done()
			}
		}(i)
	}
}

func main() {
	q := newQueue(10)

	job := Job{
		id:         1,
		jobType:    "send_mail",
		payload:    "some payload to send",
		status:     "pending",
		createdAt:  time.Now(),
		maxRetries: 3,
		retryCount: 0,
	}

	start := time.Now()

	q.startWorkers(5)

	for i := 0; i < 20; i++ {
		q.enqueue(job)
	}

	q.wg.Wait()

	fmt.Println("All jobs processed in:", time.Since(start))

	close(q.queue)
}
