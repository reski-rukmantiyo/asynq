# Asynq - Distributed Task Queue in Go

## Project Overview

Asynq is a simple, reliable, and efficient distributed task queue for Go, backed by Redis. It allows you to queue tasks and process them asynchronously with workers, supporting features like task scheduling, retries, priority queues, and more.

### Key Features
- Task queuing and asynchronous processing
- Scheduled tasks and periodic tasks
- Automatic retries with exponential backoff
- Weighted and strict priority queues
- Task deduplication with unique options
- Task grouping and aggregation
- Middleware support for handlers
- Redis Sentinel and Cluster support
- Web UI (Asynqmon) and CLI tools for monitoring
- Prometheus integration for metrics

### Architecture
Asynq follows a producer-consumer pattern:
1. **Client** - Enqueues tasks to Redis
2. **Server** - Pulls tasks from Redis and processes them with worker goroutines
3. **Scheduler** - Enqueues periodic tasks based on cron schedules
4. **Redis** - Acts as the message broker and persistence layer

## Building and Running

### Prerequisites
- Go 1.22 or later
- Redis server (version 4.0 or higher)
- Dependencies listed in `go.mod`

### Development Commands
```bash
# Run tests
go test ./...

# Run tests with race detector
go test -race ./...

# Run tests against Redis cluster
go test --redis_cluster ./...

# Lint code (requires golangci-lint)
make lint

# Generate protobuf files (if changed)
make proto
```

### Example Usage

#### 1. Define Tasks and Handlers
```go
package tasks

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"
    "github.com/hibiken/asynq"
)

// Task types
const (
    TypeEmailDelivery = "email:deliver"
    TypeImageResize   = "image:resize"
)

type EmailDeliveryPayload struct {
    UserID     int
    TemplateID string
}

type ImageResizePayload struct {
    SourceURL string
}

// Task creation functions
func NewEmailDeliveryTask(userID int, tmplID string) (*asynq.Task, error) {
    payload, err := json.Marshal(EmailDeliveryPayload{UserID: userID, TemplateID: tmplID})
    if err != nil {
        return nil, err
    }
    return asynq.NewTask(TypeEmailDelivery, payload), nil
}

func NewImageResizeTask(src string) (*asynq.Task, error) {
    payload, err := json.Marshal(ImageResizePayload{SourceURL: src})
    if err != nil {
        return nil, err
    }
    return asynq.NewTask(TypeImageResize, payload, asynq.MaxRetry(5), asynq.Timeout(20*time.Minute)), nil
}

// Task handlers
func HandleEmailDeliveryTask(ctx context.Context, t *asynq.Task) error {
    var p EmailDeliveryPayload
    if err := json.Unmarshal(t.Payload(), &p); err != nil {
        return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
    }
    log.Printf("Sending Email to User: user_id=%d, template_id=%s", p.UserID, p.TemplateID)
    // Email delivery code ...
    return nil
}

type ImageProcessor struct {
    // ... fields for struct
}

func (processor *ImageProcessor) ProcessTask(ctx context.Context, t *asynq.Task) error {
    var p ImageResizePayload
    if err := json.Unmarshal(t.Payload(), &p); err != nil {
        return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
    }
    log.Printf("Resizing image: src=%s", p.SourceURL)
    // Image resizing code ...
    return nil
}

func NewImageProcessor() *ImageProcessor {
    return &ImageProcessor{}
}
```

#### 2. Enqueue Tasks with Client
```go
package main

import (
    "log"
    "time"
    "github.com/hibiken/asynq"
    "your/app/package/tasks"
)

const redisAddr = "127.0.0.1:6379"

func main() {
    client := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr})
    defer client.Close()

    // Enqueue task to be processed immediately
    task, err := tasks.NewEmailDeliveryTask(42, "some:template:id")
    if err != nil {
        log.Fatalf("could not create task: %v", err)
    }
    info, err := client.Enqueue(task)
    if err != nil {
        log.Fatalf("could not enqueue task: %v", err)
    }
    log.Printf("enqueued task: id=%s queue=%s", info.ID, info.Queue)

    // Schedule task to be processed in the future
    info, err = client.Enqueue(task, asynq.ProcessIn(24*time.Hour))
    if err != nil {
        log.Fatalf("could not schedule task: %v", err)
    }
    log.Printf("enqueued task: id=%s queue=%s", info.ID, info.Queue)

    // Set options to tune task processing behavior
    task, err = tasks.NewImageResizeTask("https://example.com/myassets/image.jpg")
    if err != nil {
        log.Fatalf("could not create task: %v", err)
    }
    info, err = client.Enqueue(task, asynq.MaxRetry(10), asynq.Timeout(3*time.Minute))
    if err != nil {
        log.Fatalf("could not enqueue task: %v", err)
    }
    log.Printf("enqueued task: id=%s queue=%s", info.ID, info.Queue)
}
```

#### 3. Process Tasks with Server
```go
package main

import (
    "log"
    "github.com/hibiken/asynq"
    "your/app/package/tasks"
)

const redisAddr = "127.0.0.1:6379"

func main() {
    srv := asynq.NewServer(
        asynq.RedisClientOpt{Addr: redisAddr},
        asynq.Config{
            // Specify how many concurrent workers to use
            Concurrency: 10,
            // Optionally specify multiple queues with different priority
            Queues: map[string]int{
                "critical": 6,
                "default":  3,
                "low":      1,
            },
        },
    )

    // Use ServeMux to route tasks to handlers
    mux := asynq.NewServeMux()
    mux.HandleFunc(tasks.TypeEmailDelivery, tasks.HandleEmailDeliveryTask)
    mux.Handle(tasks.TypeImageResize, tasks.NewImageProcessor())
    // ...register other handlers...

    if err := srv.Run(mux); err != nil {
        log.Fatalf("could not run server: %v", err)
    }
}
```

#### 4. Schedule Periodic Tasks
```go
package main

import (
    "log"
    "time"
    "github.com/hibiken/asynq"
)

func main() {
    scheduler := asynq.NewScheduler(
        asynq.RedisClientOpt{Addr: ":6379"},
        &asynq.SchedulerOpts{Location: time.Local},
    )

    if _, err := scheduler.Register("* * * * *", asynq.NewTask("task1", nil)); err != nil {
        log.Fatal(err)
    }
    if _, err := scheduler.Register("@every 30s", asynq.NewTask("task2", nil)); err != nil {
        log.Fatal(err)
    }

    // Run blocks and waits for os signal to terminate the program
    if err := scheduler.Run(); err != nil {
        log.Fatal(err)
    }
}
```

## Development Conventions

### Code Structure
- Core functionality in root package files (`asynq.go`, `client.go`, `server.go`, etc.)
- Internal packages under `internal/` directory
- Test files alongside implementation with `_test.go` suffix
- Example usage in `example_test.go`

### Testing
- Comprehensive test suite covering all functionality
- Integration tests that require Redis server
- Use of `github.com/google/go-cmp` for comparisons
- Test helpers in `internal/testutil`

### Error Handling
- Custom error types for domain-specific errors
- Use of Go 1.13+ error wrapping with `%w` verb
- Clear error messages with context

### Redis Connection Options
Asynq supports multiple Redis connection types:
- `RedisClientOpt` - Direct connection to Redis server
- `RedisFailoverClientOpt` - Connection through Redis Sentinel
- `RedisClusterClientOpt` - Connection to Redis Cluster
- `ParseRedisURI` - Parse connection string to appropriate option type

### Task Options
Tasks can be customized with various options:
- `MaxRetry` - Maximum retry attempts
- `Queue` - Target queue name
- `Timeout` - Processing timeout duration
- `Deadline` - Absolute deadline time
- `Unique` - Deduplication with TTL
- `ProcessAt`/`ProcessIn` - Scheduling options
- `Retention` - Retention period for completed tasks
- `Group` - Task grouping for aggregation

### Handler Interface
Handlers can be implemented as:
- Functions with signature `func(context.Context, *asynq.Task) error`
- Types implementing `Handler` interface with `ProcessTask` method
- Used with `ServeMux` for routing based on task type patterns

### Middleware Support
Middleware functions can wrap handlers for cross-cutting concerns:
```go
mux := asynq.NewServeMux()
mux.Use(loggingMiddleware, authMiddleware)
```

### Contributing
See `CONTRIBUTING.md` for guidelines on:
- Reporting bugs and feature requests
- Submitting pull requests
- Running tests locally
- Code review process