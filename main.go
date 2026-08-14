package main

import (
	"fmt"
	"sync"
	"time"
)

type Job struct {
	id      int
	jobType string
	payload string
	status  string
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
	fmt.Println(job.jobType + "type, payload: " + job.payload)
	return nil
}

func (q *jobQueue) startWorkers(workers int) {
	for i := range workers {
		go func() {
			for job := range q.queue {
				fmt.Println("Worker: " + fmt.Sprint(i))
				if q.processJob(job) != nil {
					fmt.Println("worker failed to process job: " + fmt.Sprint(job.id))
				}
				q.wg.Done()
			}
		}()
	}
}

func main() {
	q := newQueue(10)

	job := Job{
		id:      1,
		jobType: "send_mail",
		payload: "some payload to send",
		status:  "pending",
	}

	start := time.Now()

	q.startWorkers(5)

	for i := 0; i < 20; i++ {
		q.enqueue(job)
	}

	q.wg.Wait()

	fmt.Println("All jobs processed in:", time.Since(start))
}
