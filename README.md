# pgjobs, a dead simple postgres job queueing mechanism

This project aims to be a blueprint for your own job queue solution with Go and PostgreSQL.
It is recommended to fork this project and adjust the job queue to your own needs.

## Example usage

First define a job, for example `jobs/emailUser.go`:

```go
package jobs

import (
	"encoding/json"
	"log"

	"github.com/rverton/pgjobs"
)

type EmailUser struct {
	Email string
}

func NewEmailUser(email string) *EmailUser {
	return &EmailUser{
		Email: email,
	}
}

// the task which should be performed
func (e EmailUser) Perform(attempt int32) error {
	log.Printf("emailing %v, attempt=%v", e.Email, attempt)
	return nil
}

// boilerplate code, you do not need to modify anything here
func (e EmailUser) Load(data string) (pgjobs.Job, error) {
	var n EmailUser
	err := json.Unmarshal([]byte(data), &n)
	return n, err
}
```

You can then setup a queue, (optionally) enforce the jobs table schema, and work on queued jobs.

```go
package main

import (
	"myproject/jobs"
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/rverton/pgjobs"
)

func main() {
    db, err := sql.Open("postgres", connStr)
    if err != nil {
        log.Fatal(err)
    }

	ctx := context.Background()

	// initiate queue with postgres connection
	queue := pgjobs.NewQueue(db)
	if err := queue.SetupSchema(ctx); err != nil {
		panic(err)
	}

	// example to enqueue a job
	job := jobs.NewEmailUser("foo@example.com")
	if err = queue.Enqueue(context.Background(), job, "default"); err != nil {
		log.Printf("error enqueueing: %v", err)
	}

	// start worker and pass all processable jobs
	queues := []string{"default"}
	if err := queue.Worker(ctx, queues, &jobs.EmailUser{}); err != nil {
		log.Println(err)
	}
}
```
