package main

import (
	"database/sql"

	_ "modernc.org/sqlite"

	"fmt"
	"sync"
	"time"
)

type JobStatus string

const (
	Pending    JobStatus = "PENDING"
	Processing JobStatus = "PROCESSING"
	Completed  JobStatus = "COMPLETED"
	Failed     JobStatus = "FAILED"
)

type Job struct {
	id           int
	jobType      string
	payload      string
	status       JobStatus
	createdAt    time.Time
	maxAttempts  int
	attemptCount int
}

type jobQueue struct {
	queue chan *Job
	wg    sync.WaitGroup
}

type jobRepository struct {
	db *sql.DB
	mu sync.Mutex
}

type Scheduler struct {
	repository *jobRepository
	tasks      *jobQueue
}

func (scheduler *Scheduler) submitJob(job *Job) {
	scheduler.repository.createJob(job)
	scheduler.tasks.enqueue(job)
}

func (scheduler *Scheduler) start(workers int) {
	scheduler.tasks.startWorkers(workers, scheduler.repository)
}

func (scheduler *Scheduler) Wait() {
	scheduler.tasks.wg.Wait()
}

func (database *jobRepository) createJobTable() {
	_, err := database.db.Exec(`
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_type TEXT NOT NULL,
        payload TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at DATETIME NOT NULL,
        max_attempts INTEGER NOT NULL,
        attempt_count INTEGER NOT NULL
    )
	`)
	if err != nil {
		panic(err)
	}
}

func (database *jobRepository) updateJobStatus(job *Job) {
	database.mu.Lock()
	defer database.mu.Unlock()
	_, err := database.db.Exec(`UPDATE jobs SET status = ?, attempt_count = ? WHERE id = ?`, job.status, job.attemptCount, job.id)

	if err != nil {
		panic(err)
	}
}

func (database *jobRepository) createJob(job *Job) {
	database.mu.Lock()
	defer database.mu.Unlock()
	result, err := database.db.Exec(`
		INSERT INTO jobs (
			job_type,
			payload,
			status,
			created_at,
			max_attempts,
			attempt_count
		)
		VALUES (?, ?, ?, ?, ?, ?)
	`,
		job.jobType,
		job.payload,
		job.status,
		job.createdAt,
		job.maxAttempts,
		job.attemptCount,
	)

	if err != nil {
		panic(err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		panic(err)
	}

	job.id = int(id)
}

func newQueue(size int) *jobQueue {
	return &jobQueue{
		queue: make(chan *Job, size),
	}
}

func (q *jobQueue) enqueue(job *Job) {
	q.wg.Add(1)
	q.queue <- job
}

func (q *jobQueue) processJob(job *Job) error {
	time.Sleep(1 * time.Second) // Simulate job processing time
	if job.id%3 == 0 {
		return fmt.Errorf("An deliberate error")
	}
	fmt.Printf("Processing job: %s, payload: %s\n", job.jobType, job.payload)
	return nil
}

func (q *jobQueue) startWorkers(workers int, repo *jobRepository) {
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			for job := range q.queue {
				fmt.Printf("Worker: %d\n", workerID)
				job.status = Processing
				job.attemptCount++
				repo.updateJobStatus(job)

				if err := q.processJob(job); err != nil {
					job.status = Failed
					repo.updateJobStatus(job)
					fmt.Printf("Worker: %d failed to process job: %d\n", workerID, job.id)

					if job.attemptCount < job.maxAttempts {
						job.status = Pending
						repo.updateJobStatus(job)

						q.wg.Add(1)
						q.queue <- job
					} else {
						fmt.Printf("Worker: %d job: %d reached max attempts\n", workerID, job.id)
					}
				} else {
					job.status = Completed
					repo.updateJobStatus(job)
				}
				q.wg.Done()
			}
		}(i)
	}
}

func main() {
	q := newQueue(10)

	start := time.Now()

	db, err := sql.Open("sqlite", "jobs.db")
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		panic(err)
	}

	database := jobRepository{db: db}
	defer db.Close()
	database.createJobTable()

	scheduler := Scheduler{repository: &database, tasks: q}

	scheduler.start(5)

	for i := 0; i < 20; i++ {
		job := Job{
			jobType:      "send_mail",
			payload:      "some payload to send",
			status:       Pending,
			createdAt:    time.Now(),
			maxAttempts:  3,
			attemptCount: 0,
		}

		scheduler.submitJob(&job)
	}

	scheduler.Wait()

	fmt.Println("All jobs processed in:", time.Since(start))

}
