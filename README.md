# GoPulse

A lightweight job scheduling and processing system built in Go. Jobs are persisted in SQLite, processed concurrently using worker pools, retried on failure, and recovered automatically after application restarts.

## Features

- Concurrent job processing with goroutines
- Configurable worker pool
- Buffered job queue
- SQLite persistence
- Job status tracking
- Automatic retries
- Startup recovery for incomplete jobs
- Cleanup of completed jobs
- Thread-safe database operations

## Job States

```text
PENDING
PROCESSING
COMPLETED
FAILED
```

## Project Structure

```text
Scheduler
 ├── Job Queue
 ├── Worker Pool
 └── Repository (SQLite)

+-------------+
|   SQLite    |
+------+------+
       ↑
+-----------+    +-----+-----+    +-----------+
| Scheduler | -> | Job Queue | -> | Workers   |
+-----------+    +-----------+    +-----------+
```

## Database Schema

```sql
CREATE TABLE jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    max_attempts INTEGER NOT NULL,
    attempt_count INTEGER NOT NULL
);
```

## Job Processing Flow

```text
Create Job
    |
    v
Store in SQLite
    |
    v
Add to Queue
    |
    v
Worker Picks Job
    |
    v
PROCESSING
    |
    +---- Success --> COMPLETED
    |
    +---- Failure --> Retry [3 times]
                        ↓
                      PENDING
```

## Recovery Flow

On startup, the scheduler:

1. Finds jobs with status `PENDING` or `PROCESSING`
2. Resets them to `PENDING`
3. Re-enqueues them for processing

This enables recovery from unexpected shutdowns without losing pending work.

## Running

```bash
go mod tidy
go run .
```

## Dependencies

```text
modernc.org/sqlite
database/sql
```

## Current Capabilities

- Persistent job storage
- Concurrent workers
- Retry mechanism
- Status management
- Startup recovery
- Completed job cleanup
- REST API
- Graceful shutdown

## Planned Enhancements

- Structured logging
- Metrics and monitoring
