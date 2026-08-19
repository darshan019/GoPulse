package main

import (
	"context"
	"database/sql"
	"net/http"
	"os"
	"os/signal"
	"syscall"

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

func (scheduler *Scheduler) fetchJob(id int) (Job, error) {
	row := scheduler.repository.db.QueryRow(`
		SELECT id,
			job_type,
			payload,
			status,
			created_at,
			max_attempts,
			attempt_count
		FROM jobs WHERE id = ?
	`, id)

	var job Job

	err := row.Scan(
		&job.id,
		&job.jobType,
		&job.payload,
		&job.status,
		&job.createdAt,
		&job.maxAttempts,
		&job.attemptCount,
	)

	if err != nil {
		return Job{}, err
	}

	return job, nil
}

func (scheduler *Scheduler) submitRecoveredJobs() error {
	jobs, err := scheduler.repository.recoverJobs()
	if err != nil {
		return err
	}

	for _, job := range jobs {
		scheduler.tasks.enqueue(job)
	}

	return nil
}

func (scheduler *Scheduler) cleanUpCompletedJobs() error {
	return scheduler.repository.cleanCompletedJobs()
}

func (scheduler *Scheduler) submitJob(job *Job) error {
	if err := scheduler.repository.createJob(job); err != nil {
		return err
	}
	scheduler.tasks.enqueue(job)
	return nil
}

func (scheduler Scheduler) fetchJobs() ([]Job, error) {
	jobs, err := scheduler.repository.fetchJobs()
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func (scheduler *Scheduler) start(workers int) {
	scheduler.tasks.startWorkers(workers, scheduler.repository)
}

func (scheduler *Scheduler) Wait() {
	scheduler.tasks.wg.Wait()
}

func (database *jobRepository) createJobTable() error {
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
		return err
	}
	return nil
}

func (database *jobRepository) fetchJobs() ([]Job, error) {
	rows, err := database.db.Query(`
		SELECT id,
			job_type,
			payload,
			status,
			created_at,
			max_attempts,
			attempt_count
		FROM jobs
	`)
	if err != nil {
		return nil, err
	}

	var jobs []Job
	defer rows.Close()

	for rows.Next() {
		job := Job{}
		err := rows.Scan(
			&job.id,
			&job.jobType,
			&job.payload,
			&job.status,
			&job.createdAt,
			&job.maxAttempts,
			&job.attemptCount,
		)

		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return jobs, nil
}

func (database *jobRepository) cleanCompletedJobs() error {
	res, err := database.db.Exec("DELETE FROM jobs WHERE status = ?", Completed)
	if err != nil {
		return err
	}
	numOfCleanup, err := res.RowsAffected()
	if err != nil {
		return err
	}

	fmt.Printf("Num of completed tasks removed is: %d\n", numOfCleanup)
	return nil
}

func (database *jobRepository) updateJobStatus(job *Job) error {
	database.mu.Lock()
	defer database.mu.Unlock()
	_, err := database.db.Exec(`UPDATE jobs SET status = ?, attempt_count = ? WHERE id = ?`, job.status, job.attemptCount, job.id)

	if err != nil {
		return err
	}
	return nil
}

func (database *jobRepository) recoverJobs() ([]*Job, error) {
	rows, err := database.db.Query(`
		SELECT 
			id,
			job_type,
			payload,
			status,
			created_at,
			max_attempts,
			attempt_count
		FROM jobs WHERE STATUS IN ("PENDING", "PROCESSING")
	`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var jobs []*Job

	for rows.Next() {
		job := &Job{}
		err := rows.Scan(
			&job.id,
			&job.jobType,
			&job.payload,
			&job.status,
			&job.createdAt,
			&job.maxAttempts,
			&job.attemptCount,
		)

		if err != nil {
			return nil, err
		}

		job.status = Pending
		if err := database.updateJobStatus(job); err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return jobs, nil

}

func (database *jobRepository) createJob(job *Job) error {
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
		return err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return err
	}

	job.id = int(id)
	return nil
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
	time.Sleep(1 * time.Second)
	fmt.Printf("Job completed. Id: %d, Payload: %s\n", job.id, job.payload)
	return nil
}

func (q *jobQueue) startWorkers(workers int, repo *jobRepository) {
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			for job := range q.queue {
				fmt.Printf("Worker: %d\n", workerID)
				job.status = Processing
				job.attemptCount++
				if err := repo.updateJobStatus(job); err != nil {
					fmt.Printf("Worker: %d failed to update job status: %v\n", workerID, err)
					q.wg.Done()
					continue
				}

				if err := q.processJob(job); err != nil {
					fmt.Printf("Worker: %d failed to process job: %d\n", workerID, job.id)

					if job.attemptCount < job.maxAttempts {
						job.status = Pending
						if err := repo.updateJobStatus(job); err != nil {
							fmt.Printf("Worker: %d failed to update job status: %v\n", workerID, err)
						}
						q.enqueue(job)
						q.wg.Done()
						continue
					} else {
						job.status = Failed
						fmt.Printf("Worker: %d job: %d reached max attempts\n", workerID, job.id)
					}
				} else {
					job.status = Completed
				}
				if err := repo.updateJobStatus(job); err != nil {
					fmt.Printf("Worker: %d failed to update job status: %v\n", workerID, err)
				}
				q.wg.Done()
			}
		}(i)
	}
}

func (scheduler *Scheduler) shutdown(srv *http.Server) {
	ctx, cancel := context.WithCancel(context.Background()) // works with WithTimeout also
	defer cancel()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	<-stop                                    // blocks, effectively allowing normal execution of program
	if err := srv.Shutdown(ctx); err != nil { // stop listening to incoming requests
		fmt.Printf("%s\n", err.Error())
	}

	scheduler.Wait() // Wait for workers to finish their tasks [when cancelled]
	fmt.Println("Workers stopped")
}

func main() {
	workers := 5

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
	if err := database.createJobTable(); err != nil {
		panic(err)
	}

	scheduler := Scheduler{repository: &database, tasks: q}

	if err := scheduler.cleanUpCompletedJobs(); err != nil {
		panic(err)
	}

	scheduler.start(workers)

	if err := scheduler.submitRecoveredJobs(); err != nil {
		panic(err)
	}

	server := Api(&scheduler)

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()

	scheduler.shutdown(server)

	fmt.Println("All jobs processed in:", time.Since(start))

}
