package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

type JobResponse struct {
	ID           int       `json:"id"`
	JobType      string    `json:"jobType"`
	Payload      string    `json:"payload"`
	Status       JobStatus `json:"status"`
	CreatedAt    time.Time `json:"createdAt"`
	MaxAttempts  int       `json:"maxAttempts"`
	AttemptCount int       `json:"attemptCount"`
}

type CreateJobRequest struct {
	JobType string `json:"jobType"`
	Payload string `json:"payload"`
}

type Server struct {
	scheduler *Scheduler
}

func (s *Server) postJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Incorrect method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CreateJobRequest
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	job := Job{
		jobType:      req.JobType,
		payload:      req.Payload,
		status:       Pending,
		createdAt:    time.Now(),
		maxAttempts:  3,
		attemptCount: 0,
	}

	if err := s.scheduler.submitJob(&job); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{
		"id":     job.id,
		"status": job.status,
	})

}

func toResponse(job Job) JobResponse {
	return JobResponse{
		ID:           job.id,
		JobType:      job.jobType,
		Payload:      job.payload,
		Status:       job.status,
		CreatedAt:    job.createdAt,
		MaxAttempts:  job.maxAttempts,
		AttemptCount: job.attemptCount,
	}
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Incorrect method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	id, err := strconv.Atoi(r.URL.Query().Get("id"))
	if err != nil {
		http.Error(w, "invalid id", http.StatusInternalServerError)
		return
	}

	job, err := s.scheduler.fetchJob(id)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	res := toResponse(job)

	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) getJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Incorrect method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	jobs, err := s.scheduler.fetchJobs()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var jobResponses []JobResponse

	for _, job := range jobs {
		jobResponse := toResponse(job)
		jobResponses = append(jobResponses, jobResponse)
	}

	if err := json.NewEncoder(w).Encode(jobResponses); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func (s *Server) jobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.postJob(w, r)
	case http.MethodGet:
		s.getJobs(w, r)
	default:
		http.Error(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
	}
}

func (s *Server) run(w http.ResponseWriter, r *http.Request) {

}

func Api(s *Scheduler) {
	server := Server{scheduler: s}

	http.HandleFunc("/jobs", server.jobs)
	http.HandleFunc("/job", server.getJob)
	err := http.ListenAndServe(":8080", nil)

	if err != nil {
		panic(err)
	}
}
